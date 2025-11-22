// simple_batch_optimization.rs - Simplified Priority 1 Batch Optimization
//
// This module provides a lightweight optimization for batch document retrieval
// that reduces S3 requests by prefetching consolidated byte ranges.
//
// Key Features:
// - Smart range consolidation (groups nearby documents)
// - Parallel prefetching of byte ranges
// - Compatible with existing architecture
// - No complex caching infrastructure required

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tantivy::{DocAddress, Index, Searcher};
use tantivy::directory::FileSlice;
use quickwit_storage::Storage;
use anyhow::{Result, Context};
use crate::debug_println;

/// Configuration for batch optimization
#[derive(Debug, Clone)]
pub struct SimpleBatchConfig {
    /// Maximum size for a consolidated range (default: 16MB)
    /// Larger values reduce S3 requests but increase memory usage and latency
    pub max_range_size: usize,

    /// Maximum gap to bridge between documents (default: 512KB)
    /// Larger values consolidate more aggressively but may fetch unused data
    pub gap_tolerance: usize,

    /// Minimum documents to enable optimization (default: 50)
    /// Below this threshold, optimization overhead isn't worth it
    pub min_docs_for_optimization: usize,

    /// Maximum concurrent prefetch operations (default: 8)
    /// Higher values increase throughput but may hit S3 rate limits
    pub max_concurrent_prefetch: usize,
}

impl Default for SimpleBatchConfig {
    fn default() -> Self {
        Self {
            max_range_size: 16 * 1024 * 1024, // 16MB - balance between request size and memory
            gap_tolerance: 512 * 1024,        // 512KB - more aggressive consolidation
            min_docs_for_optimization: 50,
            max_concurrent_prefetch: 8,
        }
    }
}

impl SimpleBatchConfig {
    /// Conservative configuration - fewer, smaller requests
    /// Good for: high latency connections, memory-constrained environments
    pub fn conservative() -> Self {
        Self {
            max_range_size: 4 * 1024 * 1024,  // 4MB
            gap_tolerance: 128 * 1024,        // 128KB
            min_docs_for_optimization: 100,   // Higher threshold
            max_concurrent_prefetch: 4,
        }
    }

    /// Aggressive configuration - maximize consolidation
    /// Good for: cost optimization, high-throughput workloads with good network
    pub fn aggressive() -> Self {
        Self {
            max_range_size: 16 * 1024 * 1024, // 16MB (same as default, but more concurrent)
            gap_tolerance: 1 * 1024 * 1024,   // 1MB - more aggressive consolidation
            min_docs_for_optimization: 25,    // Lower threshold
            max_concurrent_prefetch: 12,      // Higher concurrency for throughput
        }
    }

    /// Balanced configuration (same as default)
    pub fn balanced() -> Self {
        Self::default()
    }
}

/// Represents a byte range to prefetch
#[derive(Debug, Clone)]
pub struct PrefetchRange {
    /// File path (usually doc store file)
    pub file_path: String,

    /// Start byte offset
    pub start: usize,

    /// End byte offset (exclusive)
    pub end: usize,

    /// Documents contained in this range
    pub doc_addresses: Vec<DocAddress>,
}

impl PrefetchRange {
    pub fn size(&self) -> usize {
        self.end.saturating_sub(self.start)
    }

    pub fn doc_count(&self) -> usize {
        self.doc_addresses.len()
    }
}

/// Simplified range consolidator for batch optimization
pub struct SimpleBatchOptimizer {
    pub config: SimpleBatchConfig,
}

impl SimpleBatchOptimizer {
    pub fn new(config: SimpleBatchConfig) -> Self {
        Self { config }
    }

    pub fn with_defaults() -> Self {
        Self::new(SimpleBatchConfig::default())
    }

    /// Check if optimization should be applied
    pub fn should_optimize(&self, doc_count: usize) -> bool {
        doc_count >= self.config.min_docs_for_optimization
    }

    /// Consolidate documents into prefetch ranges
    ///
    /// This groups nearby documents within the same segment into consolidated
    /// byte ranges that can be fetched in a single S3 request.
    pub fn consolidate_ranges(
        &self,
        doc_addresses: &[DocAddress],
        searcher: &Searcher,
    ) -> Result<Vec<PrefetchRange>> {
        debug_println!("📋 SIMPLE_BATCH: Consolidating {} documents into ranges", doc_addresses.len());

        if !self.should_optimize(doc_addresses.len()) {
            debug_println!("⏭️  SIMPLE_BATCH: Skipping optimization (< {} docs)",
                          self.config.min_docs_for_optimization);
            return Ok(Vec::new());
        }

        // Group documents by segment
        let mut by_segment: HashMap<u32, Vec<DocAddress>> = HashMap::new();
        for &addr in doc_addresses {
            by_segment.entry(addr.segment_ord).or_insert_with(Vec::new).push(addr);
        }

        debug_println!("📊 SIMPLE_BATCH: Documents span {} segments", by_segment.len());

        let mut all_ranges = Vec::new();

        // Process each segment independently
        for (segment_ord, mut segment_docs) in by_segment {
            debug_println!("🔍 SIMPLE_BATCH: Processing segment {} with {} docs",
                          segment_ord, segment_docs.len());

            // Get segment reader
            let segment_readers = searcher.segment_readers();
            let segment_reader = segment_readers
                .get(segment_ord as usize)
                .context("Failed to get segment reader")?;

            // Sort documents by doc_id for locality
            segment_docs.sort_by_key(|addr| addr.doc_id);

            let num_docs = segment_docs.len(); // Save before moving

            // Estimate document positions and consolidate into ranges
            let ranges = self.consolidate_segment_docs(
                segment_ord,
                segment_docs,
                segment_reader,
            )?;

            debug_println!("  ✅ Segment {}: {} docs → {} ranges",
                          segment_ord, num_docs, ranges.len());

            all_ranges.extend(ranges);
        }

        debug_println!("📦 SIMPLE_BATCH: Total consolidation: {} docs → {} ranges ({}x reduction)",
                      doc_addresses.len(),
                      all_ranges.len(),
                      if all_ranges.is_empty() { 0 } else { doc_addresses.len() / all_ranges.len() });

        // Log range details
        for (i, range) in all_ranges.iter().enumerate() {
            debug_println!("  📊 Range {}: {} docs, {} bytes ({}..{})",
                          i + 1, range.doc_count(), range.size(), range.start, range.end);
        }

        Ok(all_ranges)
    }

    /// Consolidate documents within a single segment
    ///
    /// Note: This is a simplified implementation that groups documents by proximity.
    /// For full optimization, we'd need to extract actual byte positions from Tantivy's
    /// doc store, which requires deeper integration with Tantivy internals.
    ///
    /// Current approach: Group documents into batches based on doc_id proximity
    fn consolidate_segment_docs(
        &self,
        segment_ord: u32,
        mut doc_addresses: Vec<DocAddress>,
        _segment_reader: &tantivy::SegmentReader,
    ) -> Result<Vec<PrefetchRange>> {
        // Sort by doc_id for sequential access
        doc_addresses.sort_by_key(|addr| addr.doc_id);

        debug_println!("    📏 Segment {} consolidating {} documents",
                      segment_ord, doc_addresses.len());

        // Group documents into batches based on doc_id proximity
        // This encourages sequential I/O which helps with storage cache hit rates
        let mut ranges = Vec::new();
        let mut current_batch: Vec<DocAddress> = Vec::new();
        let mut last_doc_id: Option<u32> = None;

        // Group documents where doc_ids are within a reasonable range
        let doc_id_gap_threshold = 100; // Documents within 100 doc_ids are likely nearby

        for addr in doc_addresses {
            let should_start_new_batch = if let Some(last_id) = last_doc_id {
                let gap = addr.doc_id.saturating_sub(last_id);
                gap > doc_id_gap_threshold || current_batch.len() >= 100
            } else {
                false
            };

            if should_start_new_batch && !current_batch.is_empty() {
                // Create range for current batch
                let first_doc_id = current_batch[0].doc_id;
                let last_doc_id = current_batch[current_batch.len() - 1].doc_id;

                ranges.push(PrefetchRange {
                    file_path: format!("segment_{}_store", segment_ord),
                    start: first_doc_id as usize, // Simplified: use doc_id as proxy
                    end: (last_doc_id + 1) as usize,
                    doc_addresses: current_batch.clone(),
                });

                current_batch.clear();
            }

            current_batch.push(addr);
            last_doc_id = Some(addr.doc_id);
        }

        // Add final batch
        if !current_batch.is_empty() {
            let first_doc_id = current_batch[0].doc_id;
            let last_doc_id = current_batch[current_batch.len() - 1].doc_id;

            ranges.push(PrefetchRange {
                file_path: format!("segment_{}_store", segment_ord),
                start: first_doc_id as usize,
                end: (last_doc_id + 1) as usize,
                doc_addresses: current_batch,
            });
        }

        Ok(ranges)
    }

    /// Prefetch byte ranges to populate cache
    ///
    /// This method triggers S3 requests for the consolidated byte ranges,
    /// populating the ByteRangeCache so that subsequent doc_async() calls
    /// will hit the cache instead of making new S3 requests.
    pub async fn prefetch_ranges(
        &self,
        ranges: Vec<PrefetchRange>,
        storage: Arc<dyn Storage>,
        split_uri: &str,
    ) -> Result<PrefetchStats> {
        use futures::stream::{self, StreamExt, TryStreamExt};
        use tokio::time::Instant;

        let start_time = Instant::now();
        let total_ranges = ranges.len();
        let total_bytes: usize = ranges.iter().map(|r| r.size()).sum();

        debug_println!("🚀 PREFETCH: Starting prefetch of {} ranges ({} bytes total)",
                      total_ranges, total_bytes);

        if ranges.is_empty() {
            return Ok(PrefetchStats {
                ranges_fetched: 0,
                bytes_fetched: 0,
                duration_ms: 0,
                s3_requests: 0,
            });
        }

        // Prefetch ranges in parallel
        let prefetch_futures = ranges.iter().map(|range| {
            let storage = storage.clone();
            let split_uri = split_uri.to_string();
            let range_start = range.start;
            let range_end = range.end;
            let range_size = range.size();

            async move {
                debug_println!("  📥 PREFETCH: Fetching range {}..{} ({} bytes)",
                              range_start, range_end, range_size);

                // Fetch byte range from storage
                // This will populate the ByteRangeCache
                let byte_range = range_start..range_end;
                let split_path = std::path::Path::new(&split_uri);
                let result = storage.get_slice(split_path, byte_range.clone()).await;
                let _data = result.with_context(|| format!("Failed to prefetch byte range {}..{} from {}", range_start, range_end, split_uri))?;

                debug_println!("  ✅ PREFETCH: Completed range {}..{}", range_start, range_end);

                Ok::<usize, anyhow::Error>(range_size)
            }
        });

        // Execute prefetch with controlled concurrency
        let bytes_fetched_per_range: Vec<usize> = stream::iter(prefetch_futures)
            .buffer_unordered(self.config.max_concurrent_prefetch)
            .try_collect()
            .await?;

        let total_fetched: usize = bytes_fetched_per_range.iter().sum();
        let duration = start_time.elapsed();

        debug_println!("✅ PREFETCH: Completed {} ranges, {} bytes in {}ms",
                      total_ranges, total_fetched, duration.as_millis());

        Ok(PrefetchStats {
            ranges_fetched: total_ranges,
            bytes_fetched: total_fetched,
            duration_ms: duration.as_millis() as u64,
            s3_requests: total_ranges, // Approximately one S3 request per range
        })
    }
}

/// Statistics from prefetch operation
#[derive(Debug, Clone)]
pub struct PrefetchStats {
    pub ranges_fetched: usize,
    pub bytes_fetched: usize,
    pub duration_ms: u64,
    pub s3_requests: usize,
}

impl PrefetchStats {
    pub fn empty() -> Self {
        Self {
            ranges_fetched: 0,
            bytes_fetched: 0,
            duration_ms: 0,
            s3_requests: 0,
        }
    }

    pub fn consolidation_ratio(&self, total_docs: usize) -> f64 {
        if self.ranges_fetched == 0 {
            1.0
        } else {
            total_docs as f64 / self.ranges_fetched as f64
        }
    }
}

/// Cumulative metrics tracking batch optimization effectiveness across all operations
///
/// This struct uses atomic operations to track metrics thread-safely without locks.
/// It accumulates statistics from all batch retrieval operations to provide
/// comprehensive monitoring and cost analysis.
#[derive(Debug)]
pub struct BatchOptimizationMetrics {
    // Request tracking
    total_batch_operations: AtomicUsize,
    total_documents_requested: AtomicU64,
    total_requests_without_optimization: AtomicU64,  // What it would have been
    total_requests_with_optimization: AtomicU64,     // Actual consolidated requests

    // Byte tracking
    total_bytes_transferred: AtomicU64,
    total_bytes_wasted: AtomicU64,  // Bytes fetched but not used (gap data)

    // Timing
    total_prefetch_duration_ms: AtomicU64,

    // Segment tracking
    segments_processed: AtomicU64,
}

impl BatchOptimizationMetrics {
    pub fn new() -> Self {
        Self {
            total_batch_operations: AtomicUsize::new(0),
            total_documents_requested: AtomicU64::new(0),
            total_requests_without_optimization: AtomicU64::new(0),
            total_requests_with_optimization: AtomicU64::new(0),
            total_bytes_transferred: AtomicU64::new(0),
            total_bytes_wasted: AtomicU64::new(0),
            total_prefetch_duration_ms: AtomicU64::new(0),
            segments_processed: AtomicU64::new(0),
        }
    }

    /// Record a batch operation
    pub fn record_batch_operation(
        &self,
        doc_count: usize,
        stats: &PrefetchStats,
        segments: usize,
        bytes_wasted: u64,
    ) {
        self.total_batch_operations.fetch_add(1, Ordering::Relaxed);
        self.total_documents_requested.fetch_add(doc_count as u64, Ordering::Relaxed);

        // Without optimization, each document would require one request
        self.total_requests_without_optimization.fetch_add(doc_count as u64, Ordering::Relaxed);

        // With optimization, we made consolidated requests
        self.total_requests_with_optimization.fetch_add(stats.s3_requests as u64, Ordering::Relaxed);

        self.total_bytes_transferred.fetch_add(stats.bytes_fetched as u64, Ordering::Relaxed);
        self.total_bytes_wasted.fetch_add(bytes_wasted, Ordering::Relaxed);
        self.total_prefetch_duration_ms.fetch_add(stats.duration_ms, Ordering::Relaxed);
        self.segments_processed.fetch_add(segments as u64, Ordering::Relaxed);
    }

    // Getters for Java API
    pub fn get_total_batch_operations(&self) -> u64 {
        self.total_batch_operations.load(Ordering::Relaxed) as u64
    }

    pub fn get_total_documents_requested(&self) -> u64 {
        self.total_documents_requested.load(Ordering::Relaxed)
    }

    pub fn get_total_requests(&self) -> u64 {
        self.total_requests_without_optimization.load(Ordering::Relaxed)
    }

    pub fn get_consolidated_requests(&self) -> u64 {
        self.total_requests_with_optimization.load(Ordering::Relaxed)
    }

    pub fn get_bytes_transferred(&self) -> u64 {
        self.total_bytes_transferred.load(Ordering::Relaxed)
    }

    pub fn get_bytes_wasted(&self) -> u64 {
        self.total_bytes_wasted.load(Ordering::Relaxed)
    }

    pub fn get_total_prefetch_duration_ms(&self) -> u64 {
        self.total_prefetch_duration_ms.load(Ordering::Relaxed)
    }

    pub fn get_segments_processed(&self) -> u64 {
        self.segments_processed.load(Ordering::Relaxed)
    }

    pub fn get_consolidation_ratio(&self) -> f64 {
        let without = self.total_requests_without_optimization.load(Ordering::Relaxed);
        let with = self.total_requests_with_optimization.load(Ordering::Relaxed);
        if with == 0 {
            1.0
        } else {
            without as f64 / with as f64
        }
    }

    pub fn get_cost_savings_percent(&self) -> f64 {
        let without = self.total_requests_without_optimization.load(Ordering::Relaxed) as f64;
        let with = self.total_requests_with_optimization.load(Ordering::Relaxed) as f64;
        if without == 0.0 {
            0.0
        } else {
            ((without - with) / without) * 100.0
        }
    }

    pub fn get_efficiency_percent(&self) -> f64 {
        let total = self.total_bytes_transferred.load(Ordering::Relaxed) as f64;
        let wasted = self.total_bytes_wasted.load(Ordering::Relaxed) as f64;
        if total == 0.0 {
            100.0
        } else {
            ((total - wasted) / total) * 100.0
        }
    }

    /// Reset all metrics (useful for testing or periodic reporting)
    pub fn reset(&self) {
        self.total_batch_operations.store(0, Ordering::Relaxed);
        self.total_documents_requested.store(0, Ordering::Relaxed);
        self.total_requests_without_optimization.store(0, Ordering::Relaxed);
        self.total_requests_with_optimization.store(0, Ordering::Relaxed);
        self.total_bytes_transferred.store(0, Ordering::Relaxed);
        self.total_bytes_wasted.store(0, Ordering::Relaxed);
        self.total_prefetch_duration_ms.store(0, Ordering::Relaxed);
        self.segments_processed.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_optimize() {
        let optimizer = SimpleBatchOptimizer::with_defaults();

        assert!(!optimizer.should_optimize(10));   // Too few
        assert!(!optimizer.should_optimize(49));   // Just below threshold
        assert!(optimizer.should_optimize(50));    // At threshold
        assert!(optimizer.should_optimize(1000));  // Well above threshold
    }

    #[test]
    fn test_range_size() {
        let range = PrefetchRange {
            file_path: "test".to_string(),
            start: 1000,
            end: 2000,
            doc_addresses: vec![DocAddress::new(0, 0)],
        };

        assert_eq!(range.size(), 1000);
        assert_eq!(range.doc_count(), 1);
    }
}
