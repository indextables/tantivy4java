package io.indextables.tantivy4java.split;

/**
 * Configuration for adaptive tuning of batch optimization parameters.
 *
 * <p>Adaptive tuning automatically adjusts batch optimization parameters based on
 * observed performance metrics. It monitors consolidation ratios and waste factors
 * to find optimal settings for different workloads.
 *
 * <p>Priority 5: Adaptive Tuning - Java API
 *
 * <h3>Example Usage:</h3>
 * <pre>{@code
 * // Enable adaptive tuning with default settings
 * AdaptiveTuningConfig config = AdaptiveTuningConfig.enabled();
 *
 * // Configure cache manager with adaptive tuning
 * SplitCacheManager.CacheConfig cacheConfig =
 *     new SplitCacheManager.CacheConfig("adaptive-cache")
 *         .withMaxCacheSize(200_000_000)
 *         .withAdaptiveTuning(config);
 *
 * // Adaptive tuning will automatically adjust parameters based on workload
 * }</pre>
 *
 * <h3>How It Works:</h3>
 * <ul>
 *   <li>Monitors batch performance (consolidation ratio, waste factor, latency)</li>
 *   <li>Adjusts gap_tolerance when consolidation is too low or too high</li>
 *   <li>Adjusts max_range_size based on consolidation patterns</li>
 *   <li>Safety limits prevent extreme configurations</li>
 *   <li>Gradual adjustments avoid performance instability</li>
 * </ul>
 *
 * <h3>Performance Targets:</h3>
 * <ul>
 *   <li>Target consolidation ratio: 10x - 50x</li>
 *   <li>Gap tolerance range: 64KB - 2MB</li>
 *   <li>Max range size: 2MB - 32MB</li>
 *   <li>Adjustments made every 5+ batches</li>
 * </ul>
 */
public class AdaptiveTuningConfig {

    private final boolean enabled;
    private final int minBatchesForAdjustment;

    /**
     * Create adaptive tuning configuration.
     *
     * @param enabled whether adaptive tuning is enabled
     * @param minBatchesForAdjustment minimum batches before making adjustments
     */
    private AdaptiveTuningConfig(boolean enabled, int minBatchesForAdjustment) {
        this.enabled = enabled;
        this.minBatchesForAdjustment = minBatchesForAdjustment;
    }

    /**
     * Create adaptive tuning configuration with default settings (enabled).
     *
     * @return default adaptive tuning configuration
     */
    public static AdaptiveTuningConfig enabled() {
        return new AdaptiveTuningConfig(true, 5);
    }

    /**
     * Create disabled adaptive tuning configuration.
     *
     * @return disabled adaptive tuning configuration
     */
    public static AdaptiveTuningConfig disabled() {
        return new AdaptiveTuningConfig(false, 5);
    }

    /**
     * Create custom adaptive tuning configuration.
     *
     * @param enabled whether adaptive tuning is enabled
     * @param minBatchesForAdjustment minimum batches before making adjustments (must be &gt;= 3)
     * @return custom adaptive tuning configuration
     */
    public static AdaptiveTuningConfig custom(boolean enabled, int minBatchesForAdjustment) {
        if (minBatchesForAdjustment < 3) {
            throw new IllegalArgumentException(
                "minBatchesForAdjustment must be at least 3, got: " + minBatchesForAdjustment);
        }
        return new AdaptiveTuningConfig(enabled, minBatchesForAdjustment);
    }

    /**
     * Check if adaptive tuning is enabled.
     *
     * @return true if adaptive tuning is enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Get minimum batches required before making adjustments.
     *
     * @return minimum batches for adjustment
     */
    public int getMinBatchesForAdjustment() {
        return minBatchesForAdjustment;
    }

    @Override
    public String toString() {
        return "AdaptiveTuningConfig{" +
                "enabled=" + enabled +
                ", minBatchesForAdjustment=" + minBatchesForAdjustment +
                '}';
    }
}
