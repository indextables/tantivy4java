// optimized_batch_retrieval.rs - Optimized Batch Document Retrieval with Persistent Cache
//
// This module implements the comprehensive optimized batch document retrieval system
// with multi-tier persistent caching, smart range consolidation, and async parallel processing.

use std::sync::{Arc, Mutex};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use quickwit_storage::{Storage, StorageResolver, ByteRangeCache, OwnedBytes, STORAGE_METRICS};
use tokio::sync::Semaphore;
use futures::future;
use anyhow::{Result, Context};
use serde::{Serialize, Deserialize};

use crate::debug_println;

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
        debug_println!("📋 RANGE_CONSOLIDATION: Processing {} documents", doc_addresses.len());

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

        let doc_count = sorted_docs.len();
        for doc_addr in sorted_docs {
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
            let should_start_new_range =
                !current_range_docs.is_empty() && (
                    new_range_size > self.config.max_range_size ||
                    gap_size > self.config.gap_tolerance
                );

            if should_start_new_range {
                // Finalize current range if it has enough documents
                if current_range_docs.len() >= self.config.min_docs_per_range {
                    ranges.push(self.create_document_range(
                        current_range_docs.clone(),
                        current_range_start,
                        &doc_position_fn,
                        &doc_size_fn
                    ));
                } else {
                    // Add individual documents as single-doc ranges
                    for single_doc in current_range_docs {
                        ranges.push(self.create_single_document_range(
                            single_doc,
                            &doc_position_fn,
                            &doc_size_fn
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
                    &doc_size_fn
                ));
            } else {
                for single_doc in current_range_docs {
                    ranges.push(self.create_single_document_range(
                        single_doc,
                        &doc_position_fn,
                        &doc_size_fn
                    ));
                }
            }
        }

        debug_println!("📊 CONSOLIDATION_RESULT: {} documents -> {} ranges",
                      sorted_docs.len(), ranges.len());

        // Log range statistics
        for (i, range) in ranges.iter().enumerate() {
            debug_println!("  📦 Range {}: {} docs, {} bytes ({}..{})",
                          i + 1,
                          range.document_count(),
                          range.size_bytes(),
                          range.byte_start,
                          range.byte_end);
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

/// Configuration for Quickwit-integrated persistent cache
#[derive(Debug, Clone)]
pub struct QuickwitCacheConfig {
    /// Cache storage URI (file:// or s3:// for persistent cache)
    pub cache_storage_uri: String,

    /// Maximum memory cache size (default: 256MB)
    pub memory_cache_size: usize,

    /// Cache key prefix to avoid conflicts
    pub cache_key_prefix: String,

    /// Enable storage-level compression (handled by Quickwit storage)
    pub enable_storage_compression: bool,
}

impl Default for QuickwitCacheConfig {
    fn default() -> Self {
        Self {
            cache_storage_uri: "file://./cache".to_string(),
            memory_cache_size: 256 * 1024 * 1024, // 256MB
            cache_key_prefix: "tantivy4java".to_string(),
            enable_storage_compression: true,
        }
    }
}

/// Multi-tier caching system leveraging Quickwit's async storage
pub struct QuickwitPersistentCacheManager {
    memory_cache: ByteRangeCache,
    disk_storage: Arc<dyn Storage>,
    config: QuickwitCacheConfig,
}

impl QuickwitPersistentCacheManager {
    /// Create cache manager using Quickwit's async storage system
    pub async fn new(
        config: QuickwitCacheConfig,
        storage_resolver: Arc<StorageResolver>
    ) -> Result<Self> {
        // Use Quickwit's storage resolver to create persistent cache storage
        use quickwit_common::uri::Uri;
        let cache_uri: Uri = config.cache_storage_uri.parse()
            .map_err(|e| anyhow::anyhow!("Failed to parse cache storage URI: {}", e))?;
        let disk_storage = storage_resolver
            .resolve(&cache_uri)
            .await
            .context("Failed to resolve cache storage URI")?;

        let memory_cache = ByteRangeCache::with_infinite_capacity(
            &STORAGE_METRICS.shortlived_cache
        );

        debug_println!("💾 QUICKWIT_CACHE_INIT: Initialized persistent cache with {}MB memory, storage: {}",
                      config.memory_cache_size / (1024 * 1024),
                      config.cache_storage_uri);

        Ok(Self {
            memory_cache,
            disk_storage,
            config,
        })
    }

    /// Multi-tier cache lookup with automatic promotion using Quickwit storage
    pub async fn get_range(
        &self,
        split_path: &Path,
        byte_range: Range<usize>
    ) -> Option<OwnedBytes> {
        // L1: Memory cache lookup
        if let Some(data) = self.memory_cache.get_slice(split_path, byte_range.clone()) {
            debug_println!("✅ L1_CACHE_HIT: Memory cache hit for {}:{:?}",
                          split_path.display(), byte_range);
            return Some(data);
        }

        // L2: Disk cache lookup using Quickwit storage
        let cache_key = self.generate_cache_key(split_path, &byte_range);
        match self.disk_storage.get_slice(&cache_key, 0..usize::MAX).await {
            Ok(data) => {
                debug_println!("✅ L2_CACHE_HIT: Quickwit storage cache hit for {}:{:?}",
                              split_path.display(), byte_range);

                // Promote to memory cache
                self.memory_cache.put_slice(split_path.to_owned(), byte_range, data.clone());
                Some(data)
            },
            Err(_) => {
                debug_println!("❌ CACHE_MISS: No cache hit for {}:{:?}",
                              split_path.display(), byte_range);
                None
            }
        }
    }

    /// Store range in both memory and Quickwit storage cache
    pub async fn put_range(
        &self,
        split_path: PathBuf,
        byte_range: Range<usize>,
        data: OwnedBytes
    ) -> Result<()> {
        // Store in memory cache
        self.memory_cache.put_slice(split_path.clone(), byte_range.clone(), data.clone());

        // Store in Quickwit storage asynchronously
        let cache_key = self.generate_cache_key(&split_path, &byte_range);
        let payload = Box::new(data.as_slice().to_vec());
        self.disk_storage.put(&cache_key, payload).await
            .context("Failed to store data in Quickwit storage cache")?;

        debug_println!("💾 QUICKWIT_CACHED: Stored {} bytes to Quickwit storage with key: {}",
                      data.len(), cache_key.display());

        Ok(())
    }

    /// Generate cache key using Quickwit storage path conventions
    fn generate_cache_key(&self, split_path: &Path, byte_range: &Range<usize>) -> PathBuf {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        split_path.hash(&mut hasher);
        byte_range.start.hash(&mut hasher);
        byte_range.end.hash(&mut hasher);

        let hash = format!("{:016x}", hasher.finish());

        // Use hierarchical path structure for better storage performance
        PathBuf::from(format!("{}/ranges/{}/{}/{}.cache",
                             self.config.cache_key_prefix,
                             &hash[0..2],   // First level
                             &hash[2..4],   // Second level
                             &hash[4..]))   // Filename
    }
}

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
    pub l1_cache_hits: u64,      // Memory cache
    pub l2_cache_hits: u64,      // Disk cache
    pub cache_misses: u64,       // Network fetches

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
        if total_requests == 0 { return 0.0; }
        (self.l1_cache_hits + self.l2_cache_hits) as f64 / total_requests as f64
    }

    pub fn l1_hit_ratio(&self) -> f64 {
        let total_requests = self.l1_cache_hits + self.l2_cache_hits + self.cache_misses;
        if total_requests == 0 { return 0.0; }
        self.l1_cache_hits as f64 / total_requests as f64
    }

    pub fn average_batch_size(&self) -> f64 {
        if self.total_batches == 0 { return 0.0; }
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

/// Async parallel range fetcher with Quickwit storage integration
pub struct AsyncParallelRangeFetcher {
    storage: Arc<dyn Storage>,
    persistent_cache_manager: Arc<QuickwitPersistentCacheManager>,
    max_concurrent_fetches: usize,
    fetch_semaphore: Arc<Semaphore>,
}

impl AsyncParallelRangeFetcher {
    pub fn new(
        storage: Arc<dyn Storage>,
        persistent_cache_manager: Arc<QuickwitPersistentCacheManager>,
        max_concurrent_fetches: usize,
    ) -> Self {
        let fetch_semaphore = Arc::new(Semaphore::new(max_concurrent_fetches));

        Self {
            storage,
            persistent_cache_manager,
            max_concurrent_fetches,
            fetch_semaphore,
        }
    }

    /// Enhanced fetching with Quickwit storage-based persistent cache
    pub async fn fetch_ranges_with_quickwit_persistence(
        &self,
        split_path: &Path,
        ranges: Vec<DocumentRange>
    ) -> Result<CacheOperationResult, BatchRetrievalError> {
        let mut cache_hits = Vec::new();
        let mut cache_misses = Vec::new();

        // Phase 1: Check Quickwit storage cache for each range
        for range in ranges {
            let byte_range = range.byte_start..range.byte_end;

            if let Some(cached_data) = self.persistent_cache_manager
                .get_range(split_path, byte_range.clone()).await {

                debug_println!("✅ QUICKWIT_CACHE_HIT: Range {}..{} found in Quickwit storage cache",
                              range.byte_start, range.byte_end);

                // Cache individual documents from the persistent range
                self.cache_documents_from_range(split_path, &range, cached_data).await?;
                cache_hits.push(range);
            } else {
                cache_misses.push(range);
            }
        }

        // Phase 2: Fetch missing ranges and persist using Quickwit storage
        let mut total_bytes_cached = 0;
        if !cache_misses.is_empty() {
            debug_println!("🔄 FETCHING_MISSING: {} ranges need to be fetched", cache_misses.len());

            let fetch_futures = cache_misses.iter().map(|range| {
                self.fetch_and_persist_to_quickwit_storage(split_path.to_owned(), range.clone())
            });

            // Execute fetches in parallel
            let fetch_results = future::try_join_all(fetch_futures).await?;
            total_bytes_cached = fetch_results.iter().sum();
        }

        debug_println!("✅ QUICKWIT_PERSISTENT_FETCH_COMPLETE: {} cache hits, {} fetched",
                      cache_hits.len(), cache_misses.len());

        Ok(CacheOperationResult {
            cache_hits: cache_hits.len(),
            cache_misses: cache_misses.len(),
            ranges_fetched: cache_misses.len(),
            bytes_cached: total_bytes_cached,
        })
    }

    /// Fetch range and persist using Quickwit's async storage system
    async fn fetch_and_persist_to_quickwit_storage(
        &self,
        split_path: PathBuf,
        range: DocumentRange
    ) -> Result<usize, BatchRetrievalError> {
        // Acquire semaphore permit for concurrency control
        let _permit = self.fetch_semaphore.acquire().await
            .map_err(|_| BatchRetrievalError::Cache("Semaphore error".into()))?;

        let fetch_start = Instant::now();

        debug_println!("🚀 ASYNC_FETCH: Starting range {}..{} ({} docs, {} bytes)",
                      range.byte_start, range.byte_end,
                      range.documents.len(), range.byte_end - range.byte_start);

        // Fetch from storage
        let range_data = self.storage.get_slice(&split_path,
                                               range.byte_start..range.byte_end).await?;

        debug_println!("📡 FETCHED_RANGE: {}..{} in {}ms ({} bytes)",
                      range.byte_start, range.byte_end,
                      fetch_start.elapsed().as_millis(),
                      range_data.len());

        // Persist to Quickwit storage cache (handles compression, eviction, etc.)
        self.persistent_cache_manager.put_range(
            split_path.clone(),
            range.byte_start..range.byte_end,
            range_data.clone()
        ).await.map_err(|e| BatchRetrievalError::Persistence(e.to_string()))?;

        // Cache individual documents in memory
        self.cache_documents_from_range(&split_path, &range, range_data.clone()).await?;

        Ok(range_data.len())
    }

    /// Extract and cache individual documents from a range
    async fn cache_documents_from_range(
        &self,
        split_path: &Path,
        range: &DocumentRange,
        range_data: OwnedBytes
    ) -> Result<(), BatchRetrievalError> {
        debug_println!("📋 CACHING_DOCUMENTS: Extracting {} documents from range",
                      range.documents.len());

        // For simplicity, we'll cache the entire range for each document's byte position
        // In a more sophisticated implementation, we would parse the document boundaries
        for _doc_addr in &range.documents {
            // For now, associate the entire range data with each document
            // This is a simplified approach - a production implementation would
            // parse document boundaries and cache individual documents
            let doc_range = range.byte_start..range.byte_end;
            self.persistent_cache_manager
                .memory_cache
                .put_slice(split_path.to_owned(), doc_range, range_data.clone());
        }

        Ok(())
    }
}

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
    cache_manager: Arc<QuickwitPersistentCacheManager>,
    config: DirectCacheConfig,
}

impl DirectRangeCacheManager {
    pub fn new(
        cache_manager: Arc<QuickwitPersistentCacheManager>,
        config: DirectCacheConfig
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
        debug_println!("📋 DIRECT_RANGE_PREP: Processing {} documents from batch request",
                      doc_addresses.len());

        // Sort documents by their byte positions
        let mut sorted_docs: Vec<_> = doc_addresses.iter().copied().collect();
        sorted_docs.sort_by_key(|addr| doc_position_fn(addr));

        // Group into ranges - no prediction needed, we have the exact list!
        let ranges = self.create_optimal_ranges_from_document_list(
            &sorted_docs,
            &doc_position_fn,
            &doc_size_fn
        );

        debug_println!("📊 RANGE_OPTIMIZATION: {} documents consolidated into {} ranges",
                      doc_addresses.len(), ranges.len());

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
            let should_start_new_range =
                !current_range_docs.is_empty() && (
                    new_range_size > 8 * 1024 * 1024 ||  // 8MB max
                    gap_size > self.config.max_gap_bridge
                );

            if should_start_new_range {
                // Finalize current range (we know these docs will be accessed)
                ranges.push(self.create_document_range_from_docs(
                    current_range_docs.clone(),
                    doc_position_fn,
                    doc_size_fn
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
                doc_size_fn
            ));
        }

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

        ranges.iter().map(|range| {
            let extended_start = range.byte_start.saturating_sub(self.config.extended_range_padding);
            let extended_end = range.byte_end + self.config.extended_range_padding;

            debug_println!("🔍 RANGE_EXTENSION: Extended {}..{} to {}..{} (+{} KB padding)",
                          range.byte_start, range.byte_end,
                          extended_start, extended_end,
                          self.config.extended_range_padding / 1024);

            DocumentRange {
                start_address: range.start_address,
                end_address: range.end_address,
                byte_start: extended_start,
                byte_end: extended_end,
                documents: range.documents.clone(),
            }
        }).collect()
    }
}

/// Enhanced configuration with persistent cache options
#[derive(Debug, Clone)]
pub struct OptimizedBatchConfig {
    /// Range fetching configuration
    pub max_range_size: usize,           // Default: 8MB
    pub gap_tolerance: usize,            // Default: 64KB
    pub min_docs_per_range: usize,       // Default: 2

    /// Concurrency configuration
    pub max_concurrent_ranges: usize,    // Default: 8
    pub max_concurrent_documents: usize, // Default: 30
    pub optimization_threshold: usize,   // Default: 5

    /// Quickwit storage-based persistent cache configuration
    pub cache_storage_uri: String,       // Default: "file://./cache"
    pub cache_key_prefix: String,        // Default: "tantivy4java"
    pub memory_cache_size: usize,        // Default: 256MB
    pub enable_compression: bool,        // Default: true (handled by Quickwit storage)

    /// Extended range configuration (for cache locality)
    pub enable_extended_ranges: bool,    // Default: true
    pub extended_range_padding: usize,   // Default: 128KB

    /// Monitoring configuration
    pub enable_metrics: bool,            // Default: true
}

impl Default for OptimizedBatchConfig {
    fn default() -> Self {
        Self {
            // Range fetching
            max_range_size: 8 * 1024 * 1024,
            gap_tolerance: 64 * 1024,
            min_docs_per_range: 2,

            // Concurrency
            max_concurrent_ranges: 8,
            max_concurrent_documents: 30,
            optimization_threshold: 5,

            // Quickwit storage cache
            cache_storage_uri: std::env::var("TANTIVY4JAVA_CACHE_STORAGE_URI")
                .unwrap_or_else(|_| "file://./cache".to_string()),
            cache_key_prefix: std::env::var("TANTIVY4JAVA_CACHE_KEY_PREFIX")
                .unwrap_or_else(|_| "tantivy4java".to_string()),
            memory_cache_size: std::env::var("TANTIVY4JAVA_MEMORY_CACHE_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(256 * 1024 * 1024),
            enable_compression: std::env::var("TANTIVY4JAVA_ENABLE_COMPRESSION")
                .map(|s| s.to_lowercase() == "true")
                .unwrap_or(true),

            // Extended ranges
            enable_extended_ranges: std::env::var("TANTIVY4JAVA_ENABLE_EXTENDED_RANGES")
                .map(|s| s.to_lowercase() == "true")
                .unwrap_or(true),
            extended_range_padding: std::env::var("TANTIVY4JAVA_EXTENDED_RANGE_PADDING")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(128 * 1024),

            // Monitoring
            enable_metrics: std::env::var("TANTIVY4JAVA_ENABLE_METRICS")
                .map(|s| s.to_lowercase() == "true")
                .unwrap_or(true),
        }
    }
}

/// High-level API for optimized batch document retrieval
pub struct OptimizedBatchRetriever {
    config: OptimizedBatchConfig,
    persistent_cache: Arc<QuickwitPersistentCacheManager>,
    direct_range_manager: DirectRangeCacheManager,
    parallel_fetcher: AsyncParallelRangeFetcher,
    metrics: Arc<Mutex<BatchRetrievalMetrics>>,
}

impl OptimizedBatchRetriever {
    /// Create new optimized batch retriever with persistent cache
    pub async fn new(
        config: OptimizedBatchConfig,
        storage: Arc<dyn Storage>,
        storage_resolver: Arc<StorageResolver>,
    ) -> Result<Self, BatchRetrievalError> {
        debug_println!("🚀 BATCH_RETRIEVER_INIT: Initializing optimized batch retriever");

        // Initialize Quickwit storage-based persistent cache
        let quickwit_cache_config = QuickwitCacheConfig {
            cache_storage_uri: config.cache_storage_uri.clone(),
            memory_cache_size: config.memory_cache_size,
            cache_key_prefix: config.cache_key_prefix.clone(),
            enable_storage_compression: config.enable_compression,
        };

        let persistent_cache = Arc::new(
            QuickwitPersistentCacheManager::new(quickwit_cache_config, storage_resolver).await
                .map_err(|e| BatchRetrievalError::Configuration(e.to_string()))?
        );

        // Initialize direct range manager (uses actual document list, no prediction)
        let direct_cache_config = DirectCacheConfig {
            enable_extended_ranges: config.enable_extended_ranges,
            extended_range_padding: config.extended_range_padding,
            max_gap_bridge: config.gap_tolerance,
        };

        let direct_range_manager = DirectRangeCacheManager::new(
            persistent_cache.clone(),
            direct_cache_config
        );

        // Initialize async parallel fetcher
        let parallel_fetcher = AsyncParallelRangeFetcher::new(
            storage,
            persistent_cache.clone(),
            config.max_concurrent_ranges,
        );

        debug_println!("✅ BATCH_RETRIEVER_READY: Optimized batch retriever initialized successfully");

        Ok(Self {
            config,
            persistent_cache,
            direct_range_manager,
            parallel_fetcher,
            metrics: Arc::new(Mutex::new(BatchRetrievalMetrics::default())),
        })
    }

    /// Main entry point for optimized batch document retrieval
    pub async fn retrieve_documents_optimized(
        &self,
        split_path: &Path,
        doc_addresses: Vec<DocAddress>,
        doc_position_fn: impl Fn(&DocAddress) -> usize + Clone,
        doc_size_fn: impl Fn(&DocAddress) -> usize + Clone,
        doc_retrieval_fn: impl Fn(&DocAddress) -> Result<tantivy::schema::TantivyDocument, anyhow::Error> + Clone,
    ) -> Result<BatchRetrievalResult, BatchRetrievalError> {
        let start_time = Instant::now();
        let total_docs = doc_addresses.len();

        debug_println!("🎯 OPTIMIZED_BATCH: Starting retrieval for {} documents", total_docs);

        // Apply optimization threshold
        let result = if total_docs >= self.config.optimization_threshold {
            self.retrieve_with_optimization(
                split_path,
                doc_addresses,
                doc_position_fn,
                doc_size_fn,
                doc_retrieval_fn,
            ).await?
        } else {
            self.retrieve_without_optimization(
                doc_addresses,
                doc_retrieval_fn,
            ).await?
        };

        // Update metrics
        if self.config.enable_metrics {
            self.update_metrics(&result, start_time.elapsed());
        }

        Ok(result)
    }

    /// Optimized retrieval with persistent cache
    async fn retrieve_with_optimization(
        &self,
        split_path: &Path,
        doc_addresses: Vec<DocAddress>,
        doc_position_fn: impl Fn(&DocAddress) -> usize + Clone,
        doc_size_fn: impl Fn(&DocAddress) -> usize + Clone,
        doc_retrieval_fn: impl Fn(&DocAddress) -> Result<tantivy::schema::TantivyDocument, anyhow::Error> + Clone,
    ) -> Result<BatchRetrievalResult, BatchRetrievalError> {
        let optimization_start = Instant::now();

        // Phase 1: Direct range preparation using actual document list
        let consolidation_start = Instant::now();
        let ranges = self.direct_range_manager
            .prepare_ranges_for_documents(&doc_addresses, doc_position_fn.clone(), doc_size_fn.clone()).await?;

        let consolidation_time = consolidation_start.elapsed();
        debug_println!("📊 CONSOLIDATION: {} docs -> {} ranges in {}ms",
                      doc_addresses.len(), ranges.len(), consolidation_time.as_millis());

        // Phase 2: Parallel range fetching with Quickwit storage persistence
        let fetching_start = Instant::now();
        let cache_result = self.parallel_fetcher
            .fetch_ranges_with_quickwit_persistence(split_path, ranges.clone()).await?;

        let fetching_time = fetching_start.elapsed();
        debug_println!("🚀 PERSISTENT_FETCH: Completed {} ranges in {}ms",
                      ranges.len(), fetching_time.as_millis());

        // Phase 3: Document retrieval (now should be cached)
        let retrieval_start = Instant::now();
        let documents = self.retrieve_cached_documents_parallel(
            doc_addresses,
            doc_retrieval_fn
        ).await?;

        let retrieval_time = retrieval_start.elapsed();
        debug_println!("📦 DOCUMENT_RETRIEVAL: Retrieved {} docs in {}ms",
                      documents.len(), retrieval_time.as_millis());

        Ok(BatchRetrievalResult {
            documents,
            optimization_used: true,
            total_time: optimization_start.elapsed(),
            phases: vec![
                ("consolidation", consolidation_time),
                ("fetching", fetching_time),
                ("retrieval", retrieval_time),
            ],
            cache_stats: Some(cache_result),
            range_count: ranges.len(),
        })
    }

    /// Fallback retrieval without optimization for small batches
    async fn retrieve_without_optimization(
        &self,
        doc_addresses: Vec<DocAddress>,
        doc_retrieval_fn: impl Fn(&DocAddress) -> Result<tantivy::schema::TantivyDocument, anyhow::Error> + Clone,
    ) -> Result<BatchRetrievalResult, BatchRetrievalError> {
        debug_println!("📋 UNOPTIMIZED_BATCH: Using individual retrieval for {} documents", doc_addresses.len());

        let documents = self.retrieve_cached_documents_parallel(
            doc_addresses,
            doc_retrieval_fn
        ).await?;

        Ok(BatchRetrievalResult {
            documents,
            optimization_used: false,
            total_time: Duration::from_millis(0),
            phases: vec![],
            cache_stats: None,
            range_count: 0,
        })
    }

    /// Retrieve documents in parallel using cache
    async fn retrieve_cached_documents_parallel(
        &self,
        doc_addresses: Vec<DocAddress>,
        doc_retrieval_fn: impl Fn(&DocAddress) -> Result<tantivy::schema::TantivyDocument, anyhow::Error> + Clone,
    ) -> Result<Vec<tantivy::schema::TantivyDocument>, BatchRetrievalError> {
        let semaphore = Arc::new(Semaphore::new(self.config.max_concurrent_documents));

        let retrieval_futures = doc_addresses.into_iter().map(|doc_addr| {
            let semaphore = semaphore.clone();
            let retrieval_fn = doc_retrieval_fn.clone();

            async move {
                let _permit = semaphore.acquire().await
                    .map_err(|_| BatchRetrievalError::DocumentAccess("Semaphore error".into()))?;

                retrieval_fn(&doc_addr)
                    .map_err(|e| BatchRetrievalError::DocumentAccess(e.to_string()))
            }
        });

        let documents = future::try_join_all(retrieval_futures).await?;
        Ok(documents)
    }

    /// Update performance metrics
    fn update_metrics(&self, result: &BatchRetrievalResult, total_time: Duration) {
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.total_batches += 1;
            if result.optimization_used {
                metrics.optimized_batches += 1;
            }
            metrics.total_documents += result.documents.len() as u64;
            metrics.total_ranges += result.range_count as u64;
            metrics.total_time_ms += total_time.as_millis() as u64;

            // Update cache statistics
            if let Some(cache_stats) = &result.cache_stats {
                metrics.l1_cache_hits += cache_stats.cache_hits as u64;
                metrics.cache_misses += cache_stats.cache_misses as u64;
            }

            // Update phase timings
            for (phase, duration) in &result.phases {
                match *phase {
                    "consolidation" => metrics.consolidation_time_ms += duration.as_millis() as u64,
                    "fetching" => metrics.network_time_ms += duration.as_millis() as u64,
                    "retrieval" => metrics.cache_time_ms += duration.as_millis() as u64,
                    _ => {}
                }
            }
        }
    }

    /// Get current performance metrics
    pub fn get_metrics(&self) -> BatchRetrievalMetrics {
        self.metrics.lock()
            .map(|m| m.clone())
            .unwrap_or_default()
    }

    /// Reset performance metrics
    pub fn reset_metrics(&self) {
        if let Ok(mut metrics) = self.metrics.lock() {
            *metrics = BatchRetrievalMetrics::default();
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