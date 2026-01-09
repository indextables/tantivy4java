// retriever.rs - High-level API for optimized batch document retrieval

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::future;
use quickwit_storage::{Storage, StorageResolver};
use tokio::sync::Semaphore;

use crate::debug_println;

use super::cache::{QuickwitCacheConfig, QuickwitPersistentCacheManager};
use super::direct::{DirectCacheConfig, DirectRangeCacheManager};
use super::errors::{BatchRetrievalError, BatchRetrievalMetrics, BatchRetrievalResult};
use super::parallel::AsyncParallelRangeFetcher;
use super::types::DocAddress;

/// Enhanced configuration with persistent cache options
#[derive(Debug, Clone)]
pub struct OptimizedBatchConfig {
    /// Range fetching configuration
    pub max_range_size: usize,      // Default: 8MB
    pub gap_tolerance: usize,       // Default: 64KB
    pub min_docs_per_range: usize,  // Default: 2

    /// Concurrency configuration
    pub max_concurrent_ranges: usize,    // Default: 8
    pub max_concurrent_documents: usize, // Default: 30
    pub optimization_threshold: usize,   // Default: 5

    /// Quickwit storage-based persistent cache configuration
    pub cache_storage_uri: String, // Default: "file://./cache"
    pub cache_key_prefix: String,  // Default: "tantivy4java"
    pub memory_cache_size: usize,  // Default: 256MB
    pub enable_compression: bool,  // Default: true (handled by Quickwit storage)

    /// Extended range configuration (for cache locality)
    pub enable_extended_ranges: bool,  // Default: true
    pub extended_range_padding: usize, // Default: 128KB

    /// Monitoring configuration
    pub enable_metrics: bool, // Default: true
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
    #[allow(dead_code)]
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
            QuickwitPersistentCacheManager::new(quickwit_cache_config, storage_resolver)
                .await
                .map_err(|e| BatchRetrievalError::Configuration(e.to_string()))?,
        );

        // Initialize direct range manager (uses actual document list, no prediction)
        let direct_cache_config = DirectCacheConfig {
            enable_extended_ranges: config.enable_extended_ranges,
            extended_range_padding: config.extended_range_padding,
            max_gap_bridge: config.gap_tolerance,
        };

        let direct_range_manager =
            DirectRangeCacheManager::new(persistent_cache.clone(), direct_cache_config);

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
        doc_retrieval_fn: impl Fn(&DocAddress) -> Result<tantivy::schema::TantivyDocument, anyhow::Error>
            + Clone,
    ) -> Result<BatchRetrievalResult, BatchRetrievalError> {
        let start_time = Instant::now();
        let total_docs = doc_addresses.len();

        debug_println!(
            "🎯 OPTIMIZED_BATCH: Starting retrieval for {} documents",
            total_docs
        );

        // Apply optimization threshold
        let result = if total_docs >= self.config.optimization_threshold {
            self.retrieve_with_optimization(
                split_path,
                doc_addresses,
                doc_position_fn,
                doc_size_fn,
                doc_retrieval_fn,
            )
            .await?
        } else {
            self.retrieve_without_optimization(doc_addresses, doc_retrieval_fn)
                .await?
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
        doc_retrieval_fn: impl Fn(&DocAddress) -> Result<tantivy::schema::TantivyDocument, anyhow::Error>
            + Clone,
    ) -> Result<BatchRetrievalResult, BatchRetrievalError> {
        let optimization_start = Instant::now();

        // Phase 1: Direct range preparation using actual document list
        let consolidation_start = Instant::now();
        let ranges = self
            .direct_range_manager
            .prepare_ranges_for_documents(
                &doc_addresses,
                doc_position_fn.clone(),
                doc_size_fn.clone(),
            )
            .await?;

        let consolidation_time = consolidation_start.elapsed();
        debug_println!(
            "📊 CONSOLIDATION: {} docs -> {} ranges in {}ms",
            doc_addresses.len(),
            ranges.len(),
            consolidation_time.as_millis()
        );

        // Phase 2: Parallel range fetching with Quickwit storage persistence
        let fetching_start = Instant::now();
        let cache_result = self
            .parallel_fetcher
            .fetch_ranges_with_quickwit_persistence(split_path, ranges.clone())
            .await?;

        let fetching_time = fetching_start.elapsed();
        debug_println!(
            "🚀 PERSISTENT_FETCH: Completed {} ranges in {}ms",
            ranges.len(),
            fetching_time.as_millis()
        );

        // Phase 3: Document retrieval (now should be cached)
        let retrieval_start = Instant::now();
        let documents = self
            .retrieve_cached_documents_parallel(doc_addresses, doc_retrieval_fn)
            .await?;

        let retrieval_time = retrieval_start.elapsed();
        debug_println!(
            "📦 DOCUMENT_RETRIEVAL: Retrieved {} docs in {}ms",
            documents.len(),
            retrieval_time.as_millis()
        );

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
        doc_retrieval_fn: impl Fn(&DocAddress) -> Result<tantivy::schema::TantivyDocument, anyhow::Error>
            + Clone,
    ) -> Result<BatchRetrievalResult, BatchRetrievalError> {
        debug_println!(
            "📋 UNOPTIMIZED_BATCH: Using individual retrieval for {} documents",
            doc_addresses.len()
        );

        let documents = self
            .retrieve_cached_documents_parallel(doc_addresses, doc_retrieval_fn)
            .await?;

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
        doc_retrieval_fn: impl Fn(&DocAddress) -> Result<tantivy::schema::TantivyDocument, anyhow::Error>
            + Clone,
    ) -> Result<Vec<tantivy::schema::TantivyDocument>, BatchRetrievalError> {
        let semaphore = Arc::new(Semaphore::new(self.config.max_concurrent_documents));

        let retrieval_futures = doc_addresses.into_iter().map(|doc_addr| {
            let semaphore = semaphore.clone();
            let retrieval_fn = doc_retrieval_fn.clone();

            async move {
                let _permit = semaphore
                    .acquire()
                    .await
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
        self.metrics.lock().map(|m| m.clone()).unwrap_or_default()
    }

    /// Reset performance metrics
    pub fn reset_metrics(&self) {
        if let Ok(mut metrics) = self.metrics.lock() {
            *metrics = BatchRetrievalMetrics::default();
        }
    }
}
