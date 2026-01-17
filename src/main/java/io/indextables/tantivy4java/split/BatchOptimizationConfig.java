package io.indextables.tantivy4java.split;

/**
 * Configuration for batch document retrieval optimization.
 *
 * <p>This configuration controls how batch document retrievals are optimized to reduce
 * S3 API calls and improve performance. The optimization works by consolidating nearby
 * documents into larger byte range requests, significantly reducing the number of S3
 * GET requests required.
 *
 * <h2>Performance Impact</h2>
 * <ul>
 *   <li><b>Without optimization:</b> 1,000 docs = 1,000 S3 requests, ~3.4 seconds</li>
 *   <li><b>With optimization:</b> 1,000 docs = 50-100 S3 requests, ~1.5-2.0 seconds</li>
 *   <li><b>Improvement:</b> 1.7-2.3x faster, 90-95% fewer S3 requests</li>
 * </ul>
 *
 * <h2>Configuration Profiles</h2>
 * <p>Three preset profiles are available for common use cases:</p>
 * <ul>
 *   <li><b>Conservative:</b> Safe for high-latency connections and memory-constrained environments</li>
 *   <li><b>Balanced (default):</b> Good general-purpose configuration</li>
 *   <li><b>Aggressive:</b> Maximizes consolidation for cost optimization</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * // Use a preset profile
 * BatchOptimizationConfig config = BatchOptimizationConfig.balanced();
 *
 * // Or customize
 * BatchOptimizationConfig config = new BatchOptimizationConfig()
 *     .setMaxRangeSize(8 * 1024 * 1024)  // 8MB ranges
 *     .setGapTolerance(256 * 1024)       // 256KB gap tolerance
 *     .setMinDocsForOptimization(100)     // Activate for 100+ docs
 *     .setMaxConcurrentPrefetch(6);       // 6 parallel prefetch operations
 *
 * // Apply to cache manager
 * SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("cache")
 *     .withBatchOptimization(config);
 * }</pre>
 *
 * @see SplitCacheManager.CacheConfig#withBatchOptimization(BatchOptimizationConfig)
 */
public class BatchOptimizationConfig {

    /**
     * Maximum size for a consolidated byte range.
     *
     * <p>Larger values reduce the number of S3 requests but increase memory usage
     * and latency per request. Smaller values are safer but less efficient.
     *
     * <p><b>Trade-offs:</b></p>
     * <ul>
     *   <li><b>Larger (16-32MB):</b> Fewer S3 requests, higher throughput, more memory</li>
     *   <li><b>Smaller (2-4MB):</b> More S3 requests, lower memory, faster timeout detection</li>
     * </ul>
     *
     * <p><b>Default:</b> 16MB (balanced)</p>
     */
    private long maxRangeSize = 16 * 1024 * 1024; // 16MB

    /**
     * Maximum gap between documents to consolidate into same range.
     *
     * <p>When documents are this close together, they'll be fetched in a single
     * request even if there's unused data between them. Larger values consolidate
     * more aggressively but may fetch unnecessary data.
     *
     * <p><b>Trade-offs:</b></p>
     * <ul>
     *   <li><b>Larger (1-2MB):</b> More consolidation, fewer requests, some wasted bandwidth</li>
     *   <li><b>Smaller (128-256KB):</b> Less consolidation, more requests, minimal waste</li>
     * </ul>
     *
     * <p><b>Default:</b> 512KB (balanced)</p>
     */
    private long gapTolerance = 512 * 1024; // 512KB

    /**
     * Minimum number of documents to activate optimization.
     *
     * <p>Below this threshold, the overhead of optimization isn't worth it.
     * The optimization is transparently skipped for small batches.
     *
     * <p><b>Default:</b> 50 documents</p>
     */
    private int minDocsForOptimization = 50;

    /**
     * Maximum number of concurrent prefetch operations.
     *
     * <p>Higher values increase throughput but may hit S3 rate limits or
     * overwhelm network connections.
     *
     * <p><b>Trade-offs:</b></p>
     * <ul>
     *   <li><b>Higher (12-16):</b> Maximum throughput, risk of rate limits</li>
     *   <li><b>Lower (4-6):</b> Conservative, stable, lower throughput</li>
     * </ul>
     *
     * <p><b>Default:</b> 8 concurrent operations</p>
     */
    private int maxConcurrentPrefetch = 8;

    /**
     * Whether optimization is enabled.
     *
     * <p><b>Default:</b> true</p>
     */
    private boolean enabled = true;

    /**
     * Minimum number of documents to use byte buffer protocol for batch retrieval.
     *
     * <p>Below this threshold, the traditional Document[] method is used.
     * Above this threshold, the byte buffer protocol reduces JNI overhead by
     * returning all documents in a single serialized buffer instead of creating
     * individual Document JNI objects.
     *
     * <p><b>Benchmark Results:</b> ByteBuffer protocol is 2-12x faster than the
     * traditional JNI method across all tested scenarios (1-5000 docs, 5-600 fields).
     * The speedup increases with more documents and fields.
     *
     * <p><b>Default:</b> 1 (always use ByteBuffer protocol, as it is always faster)</p>
     */
    private int byteBufferThreshold = 1;

    /**
     * Creates a configuration with default (balanced) settings.
     */
    public BatchOptimizationConfig() {
        // Uses default field values
    }

    /**
     * Creates a conservative configuration suitable for:
     * <ul>
     *   <li>High-latency network connections</li>
     *   <li>Memory-constrained environments</li>
     *   <li>Cautious production rollouts</li>
     * </ul>
     *
     * <p><b>Settings:</b></p>
     * <ul>
     *   <li>Max range size: 4MB (smaller requests)</li>
     *   <li>Gap tolerance: 128KB (less aggressive consolidation)</li>
     *   <li>Min docs: 100 (higher threshold)</li>
     *   <li>Max concurrent: 4 (conservative parallelism)</li>
     * </ul>
     *
     * @return conservative configuration
     */
    public static BatchOptimizationConfig conservative() {
        return new BatchOptimizationConfig()
                .setMaxRangeSize(4 * 1024 * 1024)      // 4MB
                .setGapTolerance(128 * 1024)            // 128KB
                .setMinDocsForOptimization(100)         // Higher threshold
                .setMaxConcurrentPrefetch(4);           // Conservative concurrency
    }

    /**
     * Creates a balanced configuration suitable for most production workloads.
     *
     * <p>This is the default configuration, providing a good balance between
     * performance, cost savings, and resource usage.
     *
     * <p><b>Settings:</b></p>
     * <ul>
     *   <li>Max range size: 16MB</li>
     *   <li>Gap tolerance: 512KB</li>
     *   <li>Min docs: 50</li>
     *   <li>Max concurrent: 8</li>
     * </ul>
     *
     * @return balanced configuration (same as default constructor)
     */
    public static BatchOptimizationConfig balanced() {
        return new BatchOptimizationConfig(); // Uses defaults
    }

    /**
     * Creates an aggressive configuration suitable for:
     * <ul>
     *   <li>Cost optimization (minimize S3 requests)</li>
     *   <li>High-throughput workloads</li>
     *   <li>Good network conditions</li>
     * </ul>
     *
     * <p><b>Settings:</b></p>
     * <ul>
     *   <li>Max range size: 16MB (same as balanced)</li>
     *   <li>Gap tolerance: 1MB (more aggressive consolidation)</li>
     *   <li>Min docs: 25 (lower threshold)</li>
     *   <li>Max concurrent: 12 (higher parallelism)</li>
     * </ul>
     *
     * @return aggressive configuration
     */
    public static BatchOptimizationConfig aggressive() {
        return new BatchOptimizationConfig()
                .setMaxRangeSize(16 * 1024 * 1024)      // 16MB
                .setGapTolerance(1 * 1024 * 1024)       // 1MB
                .setMinDocsForOptimization(25)          // Lower threshold
                .setMaxConcurrentPrefetch(12);          // Higher concurrency
    }

    /**
     * Creates a disabled configuration.
     *
     * <p>Use this to completely disable batch optimization while keeping
     * the configuration infrastructure in place.
     *
     * @return disabled configuration
     */
    public static BatchOptimizationConfig disabled() {
        return new BatchOptimizationConfig().setEnabled(false);
    }

    // Getters

    public long getMaxRangeSize() {
        return maxRangeSize;
    }

    public long getGapTolerance() {
        return gapTolerance;
    }

    public int getMinDocsForOptimization() {
        return minDocsForOptimization;
    }

    public int getMaxConcurrentPrefetch() {
        return maxConcurrentPrefetch;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public int getByteBufferThreshold() {
        return byteBufferThreshold;
    }

    // Fluent setters

    /**
     * Sets the maximum size for consolidated byte ranges.
     *
     * @param maxRangeSize maximum range size in bytes (must be > 0)
     * @return this configuration for method chaining
     * @throws IllegalArgumentException if maxRangeSize <= 0
     */
    public BatchOptimizationConfig setMaxRangeSize(long maxRangeSize) {
        if (maxRangeSize <= 0) {
            throw new IllegalArgumentException("maxRangeSize must be positive, got: " + maxRangeSize);
        }
        this.maxRangeSize = maxRangeSize;
        return this;
    }

    /**
     * Sets the maximum gap between documents for consolidation.
     *
     * @param gapTolerance gap tolerance in bytes (must be >= 0)
     * @return this configuration for method chaining
     * @throws IllegalArgumentException if gapTolerance < 0
     */
    public BatchOptimizationConfig setGapTolerance(long gapTolerance) {
        if (gapTolerance < 0) {
            throw new IllegalArgumentException("gapTolerance must be non-negative, got: " + gapTolerance);
        }
        this.gapTolerance = gapTolerance;
        return this;
    }

    /**
     * Sets the minimum number of documents to activate optimization.
     *
     * @param minDocs minimum document count (must be > 0)
     * @return this configuration for method chaining
     * @throws IllegalArgumentException if minDocs <= 0
     */
    public BatchOptimizationConfig setMinDocsForOptimization(int minDocs) {
        if (minDocs <= 0) {
            throw new IllegalArgumentException("minDocsForOptimization must be positive, got: " + minDocs);
        }
        this.minDocsForOptimization = minDocs;
        return this;
    }

    /**
     * Sets the maximum number of concurrent prefetch operations.
     *
     * @param maxConcurrent maximum concurrent operations (must be > 0)
     * @return this configuration for method chaining
     * @throws IllegalArgumentException if maxConcurrent <= 0
     */
    public BatchOptimizationConfig setMaxConcurrentPrefetch(int maxConcurrent) {
        if (maxConcurrent <= 0) {
            throw new IllegalArgumentException("maxConcurrentPrefetch must be positive, got: " + maxConcurrent);
        }
        this.maxConcurrentPrefetch = maxConcurrent;
        return this;
    }

    /**
     * Enables or disables the optimization.
     *
     * @param enabled true to enable, false to disable
     * @return this configuration for method chaining
     */
    public BatchOptimizationConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Sets the minimum number of documents to use byte buffer protocol.
     *
     * <p>Below this threshold, the traditional Document[] method is used.
     * Above this threshold, documents are returned in a serialized byte buffer
     * to reduce JNI overhead.
     *
     * @param threshold minimum document count to use byte buffer (must be >= 1)
     * @return this configuration for method chaining
     * @throws IllegalArgumentException if threshold < 1
     */
    public BatchOptimizationConfig setByteBufferThreshold(int threshold) {
        if (threshold < 1) {
            throw new IllegalArgumentException("byteBufferThreshold must be at least 1, got: " + threshold);
        }
        this.byteBufferThreshold = threshold;
        return this;
    }

    @Override
    public String toString() {
        return "BatchOptimizationConfig{" +
                "enabled=" + enabled +
                ", maxRangeSize=" + (maxRangeSize / 1024 / 1024) + "MB" +
                ", gapTolerance=" + (gapTolerance / 1024) + "KB" +
                ", minDocsForOptimization=" + minDocsForOptimization +
                ", maxConcurrentPrefetch=" + maxConcurrentPrefetch +
                ", byteBufferThreshold=" + byteBufferThreshold +
                '}';
    }

    /**
     * Validates this configuration.
     *
     * @throws IllegalStateException if configuration is invalid
     */
    public void validate() {
        if (maxRangeSize <= 0) {
            throw new IllegalStateException("maxRangeSize must be positive");
        }
        if (gapTolerance < 0) {
            throw new IllegalStateException("gapTolerance must be non-negative");
        }
        if (minDocsForOptimization <= 0) {
            throw new IllegalStateException("minDocsForOptimization must be positive");
        }
        if (maxConcurrentPrefetch <= 0) {
            throw new IllegalStateException("maxConcurrentPrefetch must be positive");
        }
        if (byteBufferThreshold < 1) {
            throw new IllegalStateException("byteBufferThreshold must be at least 1");
        }
        if (maxRangeSize > 100 * 1024 * 1024) {
            throw new IllegalStateException(
                    "maxRangeSize should not exceed 100MB for safety, got: " +
                            (maxRangeSize / 1024 / 1024) + "MB");
        }
        if (gapTolerance > 10 * 1024 * 1024) {
            throw new IllegalStateException(
                    "gapTolerance should not exceed 10MB to avoid excessive waste, got: " +
                            (gapTolerance / 1024 / 1024) + "MB");
        }
    }
}
