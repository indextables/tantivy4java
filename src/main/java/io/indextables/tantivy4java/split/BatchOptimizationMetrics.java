package io.indextables.tantivy4java.split;

/**
 * Metrics tracking batch optimization effectiveness across all operations.
 *
 * <p>This class provides comprehensive visibility into how batch retrieval optimization
 * is performing in production, including cost savings, efficiency, and performance metrics.
 *
 * <p><strong>Usage Example:</strong>
 * <pre>{@code
 * // Get global metrics
 * BatchOptimizationMetrics metrics = SplitCacheManager.getBatchMetrics();
 *
 * // Check cost savings
 * System.out.println("S3 Cost Savings: " + metrics.getCostSavingsPercent() + "%");
 * System.out.println("Consolidation Ratio: " + metrics.getConsolidationRatio() + "x");
 *
 * // Monitor efficiency
 * System.out.println("Bandwidth Efficiency: " + metrics.getEfficiencyPercent() + "%");
 * System.out.println("Total Bytes Transferred: " + metrics.getBytesTransferred());
 *
 * // Track operations
 * System.out.println("Batch Operations: " + metrics.getTotalBatchOperations());
 * System.out.println("Documents Retrieved: " + metrics.getTotalDocumentsRequested());
 * }</pre>
 *
 * <p><strong>Key Metrics:</strong>
 * <ul>
 *   <li><strong>Cost Savings</strong>: Percentage reduction in S3 requests (90-95% typical)</li>
 *   <li><strong>Consolidation Ratio</strong>: How many requests were avoided (10-20x typical)</li>
 *   <li><strong>Efficiency</strong>: Percentage of transferred bytes that were actually used</li>
 *   <li><strong>Throughput</strong>: Total documents and bytes processed</li>
 * </ul>
 *
 * @see SplitCacheManager#getBatchMetrics()
 * @see BatchOptimizationConfig
 */
public class BatchOptimizationMetrics {

    // Cached values retrieved from native layer
    private final long totalBatchOperations;
    private final long totalDocumentsRequested;
    private final long totalRequests;
    private final long consolidatedRequests;
    private final long bytesTransferred;
    private final long bytesWasted;
    private final long totalPrefetchDurationMs;
    private final long segmentsProcessed;
    private final double consolidationRatio;
    private final double costSavingsPercent;
    private final double efficiencyPercent;

    /**
     * Package-private constructor - metrics are obtained through SplitCacheManager.getBatchMetrics()
     */
    BatchOptimizationMetrics() {
        // Query native layer for all metrics
        this.totalBatchOperations = SplitCacheManager.nativeGetBatchMetricsTotalOperations();
        this.totalDocumentsRequested = SplitCacheManager.nativeGetBatchMetricsTotalDocuments();
        this.totalRequests = SplitCacheManager.nativeGetBatchMetricsTotalRequests();
        this.consolidatedRequests = SplitCacheManager.nativeGetBatchMetricsConsolidatedRequests();
        this.bytesTransferred = SplitCacheManager.nativeGetBatchMetricsBytesTransferred();
        this.bytesWasted = SplitCacheManager.nativeGetBatchMetricsBytesWasted();
        this.totalPrefetchDurationMs = SplitCacheManager.nativeGetBatchMetricsTotalPrefetchDuration();
        this.segmentsProcessed = SplitCacheManager.nativeGetBatchMetricsSegmentsProcessed();
        this.consolidationRatio = SplitCacheManager.nativeGetBatchMetricsConsolidationRatio();
        this.costSavingsPercent = SplitCacheManager.nativeGetBatchMetricsCostSavingsPercent();
        this.efficiencyPercent = SplitCacheManager.nativeGetBatchMetricsEfficiencyPercent();
    }

    /**
     * Total number of batch retrieval operations performed.
     *
     * <p>Each call to {@code docBatch()} that triggers optimization counts as one operation.
     *
     * @return number of batch operations
     */
    public long getTotalBatchOperations() {
        return totalBatchOperations;
    }

    /**
     * Total number of documents requested across all batch operations.
     *
     * @return cumulative document count
     */
    public long getTotalDocumentsRequested() {
        return totalDocumentsRequested;
    }

    /**
     * Total S3 requests that would have been made without optimization.
     *
     * <p>Without optimization, each document would require one S3 request.
     * This is the baseline for calculating cost savings.
     *
     * @return requests without optimization (equals totalDocumentsRequested)
     */
    public long getTotalRequests() {
        return totalRequests;
    }

    /**
     * Actual S3 requests made after consolidation.
     *
     * <p>This is typically 5-10% of what would have been required without optimization.
     *
     * @return consolidated request count
     */
    public long getConsolidatedRequests() {
        return consolidatedRequests;
    }

    /**
     * Total bytes transferred from S3 across all batch operations.
     *
     * <p>This includes both useful data and gap bytes fetched to consolidate ranges.
     *
     * @return bytes transferred
     */
    public long getBytesTransferred() {
        return bytesTransferred;
    }

    /**
     * Estimated bytes wasted (gap data fetched but not used).
     *
     * <p>When consolidating nearby documents, sometimes gap data is fetched to
     * reduce the number of S3 requests. This metric tracks that overhead.
     *
     * @return estimated wasted bytes
     */
    public long getBytesWasted() {
        return bytesWasted;
    }

    /**
     * Total time spent prefetching consolidated byte ranges.
     *
     * <p>This is the cumulative time for all prefetch operations, not including
     * subsequent document deserialization.
     *
     * @return prefetch duration in milliseconds
     */
    public long getTotalPrefetchDurationMs() {
        return totalPrefetchDurationMs;
    }

    /**
     * Total number of segments processed across all batch operations.
     *
     * <p>Documents are grouped by segment for efficient retrieval.
     *
     * @return segment count
     */
    public long getSegmentsProcessed() {
        return segmentsProcessed;
    }

    /**
     * Request consolidation ratio.
     *
     * <p>How many S3 requests were avoided per actual request made. Higher is better.
     * Typical values: 10-20x for well-optimized batches.
     *
     * <p><strong>Formula:</strong> {@code totalRequests / consolidatedRequests}
     *
     * @return consolidation ratio (e.g., 15.0 means 15 requests consolidated into 1)
     */
    public double getConsolidationRatio() {
        return consolidationRatio;
    }

    /**
     * Percentage of S3 cost savings achieved.
     *
     * <p>Compares actual requests made vs. what would have been required without optimization.
     * Typical values: 90-95% for production workloads.
     *
     * <p><strong>Formula:</strong> {@code ((totalRequests - consolidatedRequests) / totalRequests) * 100}
     *
     * @return cost savings as percentage (0-100)
     */
    public double getCostSavingsPercent() {
        return costSavingsPercent;
    }

    /**
     * Bandwidth efficiency percentage.
     *
     * <p>What percentage of transferred bytes were actually used (vs. wasted gap data).
     * Higher is better. Typical values: 85-95%.
     *
     * <p><strong>Formula:</strong> {@code ((bytesTransferred - bytesWasted) / bytesTransferred) * 100}
     *
     * @return efficiency percentage (0-100)
     */
    public double getEfficiencyPercent() {
        return efficiencyPercent;
    }

    /**
     * Average documents per batch operation.
     *
     * @return average batch size
     */
    public double getAverageBatchSize() {
        if (totalBatchOperations == 0) {
            return 0.0;
        }
        return (double) totalDocumentsRequested / totalBatchOperations;
    }

    /**
     * Average prefetch duration per batch operation.
     *
     * @return average duration in milliseconds
     */
    public double getAveragePrefetchDurationMs() {
        if (totalBatchOperations == 0) {
            return 0.0;
        }
        return (double) totalPrefetchDurationMs / totalBatchOperations;
    }

    /**
     * Estimated S3 cost savings in dollars (assuming $0.0004 per 1,000 requests).
     *
     * <p><strong>Note:</strong> Actual S3 pricing varies by region and volume.
     * This is an estimate based on standard GET request pricing.
     *
     * @return estimated cost savings in USD
     */
    public double getEstimatedCostSavingsUSD() {
        long savedRequests = totalRequests - consolidatedRequests;
        return (savedRequests / 1000.0) * 0.0004; // $0.0004 per 1,000 requests
    }

    @Override
    public String toString() {
        return String.format(
            "BatchOptimizationMetrics{" +
            "operations=%d, " +
            "documents=%d, " +
            "consolidation=%.1fx, " +
            "costSavings=%.1f%%, " +
            "efficiency=%.1f%%, " +
            "bytesTransferred=%d, " +
            "prefetchDuration=%dms" +
            "}",
            totalBatchOperations,
            totalDocumentsRequested,
            consolidationRatio,
            costSavingsPercent,
            efficiencyPercent,
            bytesTransferred,
            totalPrefetchDurationMs
        );
    }

    /**
     * Formatted summary for logging and monitoring.
     *
     * @return multi-line summary string
     */
    public String getSummary() {
        return String.format(
            "Batch Optimization Metrics:\n" +
            "  Total Operations: %,d\n" +
            "  Documents Retrieved: %,d (avg %.1f per batch)\n" +
            "  S3 Requests: %,d → %,d (%.1fx consolidation)\n" +
            "  Cost Savings: %.1f%% (~$%.2f)\n" +
            "  Bandwidth Efficiency: %.1f%%\n" +
            "  Data Transferred: %,d bytes (%,d wasted)\n" +
            "  Prefetch Time: %,dms (avg %.1fms per batch)\n" +
            "  Segments Processed: %,d",
            totalBatchOperations,
            totalDocumentsRequested,
            getAverageBatchSize(),
            totalRequests,
            consolidatedRequests,
            consolidationRatio,
            costSavingsPercent,
            getEstimatedCostSavingsUSD(),
            efficiencyPercent,
            bytesTransferred,
            bytesWasted,
            totalPrefetchDurationMs,
            getAveragePrefetchDurationMs(),
            segmentsProcessed
        );
    }
}
