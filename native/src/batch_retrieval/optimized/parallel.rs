// parallel.rs - Async parallel range fetcher with Quickwit storage integration

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use futures::future;
use quickwit_storage::{OwnedBytes, Storage};
use tokio::sync::Semaphore;

use crate::debug_println;

use super::cache::QuickwitPersistentCacheManager;
use super::errors::{BatchRetrievalError, CacheOperationResult};
use super::types::DocumentRange;

/// Async parallel range fetcher with Quickwit storage integration
pub struct AsyncParallelRangeFetcher {
    storage: Arc<dyn Storage>,
    persistent_cache_manager: Arc<QuickwitPersistentCacheManager>,
    #[allow(dead_code)]
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
        ranges: Vec<DocumentRange>,
    ) -> Result<CacheOperationResult, BatchRetrievalError> {
        let mut cache_hits = Vec::new();
        let mut cache_misses = Vec::new();

        // Phase 1: Check Quickwit storage cache for each range
        for range in ranges {
            let byte_range = range.byte_start..range.byte_end;

            if let Some(cached_data) = self
                .persistent_cache_manager
                .get_range(split_path, byte_range.clone())
                .await
            {
                debug_println!(
                    "✅ QUICKWIT_CACHE_HIT: Range {}..{} found in Quickwit storage cache",
                    range.byte_start,
                    range.byte_end
                );

                // Cache individual documents from the persistent range
                self.cache_documents_from_range(split_path, &range, cached_data)
                    .await?;
                cache_hits.push(range);
            } else {
                cache_misses.push(range);
            }
        }

        // Phase 2: Fetch missing ranges and persist using Quickwit storage
        let mut total_bytes_cached = 0;
        if !cache_misses.is_empty() {
            debug_println!(
                "🔄 FETCHING_MISSING: {} ranges need to be fetched",
                cache_misses.len()
            );

            let fetch_futures = cache_misses
                .iter()
                .map(|range| self.fetch_and_persist_to_quickwit_storage(split_path.to_owned(), range.clone()));

            // Execute fetches in parallel
            let fetch_results = future::try_join_all(fetch_futures).await?;
            total_bytes_cached = fetch_results.iter().sum();
        }

        debug_println!(
            "✅ QUICKWIT_PERSISTENT_FETCH_COMPLETE: {} cache hits, {} fetched",
            cache_hits.len(),
            cache_misses.len()
        );

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
        range: DocumentRange,
    ) -> Result<usize, BatchRetrievalError> {
        // Acquire semaphore permit for concurrency control
        let _permit = self
            .fetch_semaphore
            .acquire()
            .await
            .map_err(|_| BatchRetrievalError::Cache("Semaphore error".into()))?;

        let fetch_start = Instant::now();

        debug_println!(
            "🚀 ASYNC_FETCH: Starting range {}..{} ({} docs, {} bytes)",
            range.byte_start,
            range.byte_end,
            range.documents.len(),
            range.byte_end - range.byte_start
        );

        // Fetch from storage
        let range_data = self
            .storage
            .get_slice(&split_path, range.byte_start..range.byte_end)
            .await?;

        debug_println!(
            "📡 FETCHED_RANGE: {}..{} in {}ms ({} bytes)",
            range.byte_start,
            range.byte_end,
            fetch_start.elapsed().as_millis(),
            range_data.len()
        );

        // Persist to Quickwit storage cache (handles compression, eviction, etc.)
        self.persistent_cache_manager
            .put_range(
                split_path.clone(),
                range.byte_start..range.byte_end,
                range_data.clone(),
            )
            .await
            .map_err(|e| BatchRetrievalError::Persistence(e.to_string()))?;

        // Cache individual documents in memory
        self.cache_documents_from_range(&split_path, &range, range_data.clone())
            .await?;

        Ok(range_data.len())
    }

    /// Extract and cache individual documents from a range
    async fn cache_documents_from_range(
        &self,
        split_path: &Path,
        range: &DocumentRange,
        range_data: OwnedBytes,
    ) -> Result<(), BatchRetrievalError> {
        debug_println!(
            "📋 CACHING_DOCUMENTS: Extracting {} documents from range",
            range.documents.len()
        );

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
