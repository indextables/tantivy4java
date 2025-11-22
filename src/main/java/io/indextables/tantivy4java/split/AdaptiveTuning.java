package io.indextables.tantivy4java.split;

/**
 * Adaptive Tuning Engine for automatic batch optimization parameter adjustment.
 *
 * <p>This class provides adaptive tuning that automatically adjusts batch optimization
 * parameters based on observed performance metrics. It monitors consolidation ratios
 * and waste factors to find optimal settings for different workloads.
 *
 * <p>Priority 5: Adaptive Tuning - JNI Bridge to Native Engine
 *
 * <h3>Example Usage:</h3>
 * <pre>{@code
 * // Create adaptive tuning engine with default settings
 * AdaptiveTuningConfig config = AdaptiveTuningConfig.enabled();
 * try (AdaptiveTuning tuning = AdaptiveTuning.create(config)) {
 *
 *     // Record batch performance metrics
 *     tuning.recordBatch(100, 5, 1_000_000, 900_000, 150);
 *
 *     // Get statistics
 *     AdaptiveTuningStats stats = tuning.getStats();
 *     System.out.println("Avg consolidation: " + stats.getAvgConsolidation());
 *     System.out.println("Current gap tolerance: " + stats.getCurrentGapToleranceKB() + "KB");
 *
 *     // Adaptive tuning automatically adjusts parameters based on performance
 * }
 * }</pre>
 *
 * <h3>How It Works:</h3>
 * <ul>
 *   <li>Monitors batch performance metrics (consolidation ratio, waste factor)</li>
 *   <li>Automatically adjusts gap_tolerance when consolidation is too low or too high</li>
 *   <li>Safety limits prevent extreme configurations (64KB - 2MB gap tolerance)</li>
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
public class AdaptiveTuning implements AutoCloseable {

    private long nativePtr;
    private boolean closed = false;

    /**
     * Private constructor - use static factory methods.
     *
     * @param nativePtr pointer to native adaptive tuning engine
     */
    private AdaptiveTuning(long nativePtr) {
        this.nativePtr = nativePtr;
    }

    /**
     * Create adaptive tuning engine from configuration.
     *
     * @param config adaptive tuning configuration
     * @return new adaptive tuning engine
     */
    public static AdaptiveTuning create(AdaptiveTuningConfig config) {
        long ptr = nativeCreate(config.isEnabled(), config.getMinBatchesForAdjustment());
        return new AdaptiveTuning(ptr);
    }

    /**
     * Create adaptive tuning engine with default settings (enabled).
     *
     * @return new adaptive tuning engine with default configuration
     */
    public static AdaptiveTuning createDefault() {
        return create(AdaptiveTuningConfig.enabled());
    }

    /**
     * Create disabled adaptive tuning engine.
     *
     * @return new disabled adaptive tuning engine
     */
    public static AdaptiveTuning createDisabled() {
        return create(AdaptiveTuningConfig.disabled());
    }

    /**
     * Record batch performance metrics for adaptive adjustment.
     *
     * @param documentCount number of documents in batch
     * @param consolidatedRanges number of consolidated ranges created
     * @param bytesFetched total bytes fetched from storage
     * @param bytesUsed total bytes actually used (document data)
     * @param latencyMs batch processing time in milliseconds
     */
    public void recordBatch(
            int documentCount,
            int consolidatedRanges,
            long bytesFetched,
            long bytesUsed,
            long latencyMs) {
        checkNotClosed();
        nativeRecordBatch(nativePtr, documentCount, consolidatedRanges,
            bytesFetched, bytesUsed, latencyMs);
    }

    /**
     * Get current adaptive tuning statistics.
     *
     * @return statistics snapshot showing current tuning state
     */
    public AdaptiveTuningStats getStats() {
        checkNotClosed();
        return nativeGetStats(nativePtr);
    }

    /**
     * Enable or disable adaptive tuning.
     *
     * <p>When disabled, metrics are still tracked but parameters are not adjusted.
     *
     * @param enabled true to enable adaptive tuning
     */
    public void setEnabled(boolean enabled) {
        checkNotClosed();
        nativeSetEnabled(nativePtr, enabled);
    }

    /**
     * Check if adaptive tuning is enabled.
     *
     * @return true if adaptive tuning is enabled
     */
    public boolean isEnabled() {
        checkNotClosed();
        return nativeIsEnabled(nativePtr);
    }

    /**
     * Get current gap tolerance in bytes.
     *
     * <p>This value is automatically adjusted by adaptive tuning based on performance.
     *
     * @return current gap tolerance in bytes
     */
    public long getGapTolerance() {
        checkNotClosed();
        return nativeGetGapTolerance(nativePtr);
    }

    /**
     * Get current maximum range size in bytes.
     *
     * <p>This value is automatically adjusted by adaptive tuning based on performance.
     *
     * @return current max range size in bytes
     */
    public long getMaxRangeSize() {
        checkNotClosed();
        return nativeGetMaxRangeSize(nativePtr);
    }

    /**
     * Reset adaptive tuning history.
     *
     * <p>Clears all tracked batch metrics. Useful when workload characteristics change
     * significantly or for testing.
     */
    public void reset() {
        checkNotClosed();
        nativeReset(nativePtr);
    }

    /**
     * Check if this adaptive tuning engine has been closed.
     *
     * @return true if closed
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Verify engine is not closed.
     *
     * @throws IllegalStateException if engine is closed
     */
    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("AdaptiveTuning engine has been closed");
        }
    }

    @Override
    public void close() {
        if (!closed) {
            nativeDestroy(nativePtr);
            closed = true;
            nativePtr = 0;
        }
    }

    // ============================================================================
    // Native Methods
    // ============================================================================

    /**
     * Create native adaptive tuning engine.
     *
     * @param enabled whether adaptive tuning is enabled
     * @param minBatchesForAdjustment minimum batches before making adjustments
     * @return pointer to native engine
     */
    private static native long nativeCreate(boolean enabled, int minBatchesForAdjustment);

    /**
     * Record batch performance metrics.
     *
     * @param ptr native engine pointer
     * @param documentCount number of documents
     * @param consolidatedRanges number of consolidated ranges
     * @param bytesFetched total bytes fetched
     * @param bytesUsed total bytes used
     * @param latencyMs processing time in milliseconds
     */
    private static native void nativeRecordBatch(
        long ptr, int documentCount, int consolidatedRanges,
        long bytesFetched, long bytesUsed, long latencyMs);

    /**
     * Get adaptive tuning statistics.
     *
     * @param ptr native engine pointer
     * @return statistics snapshot
     */
    private static native AdaptiveTuningStats nativeGetStats(long ptr);

    /**
     * Enable or disable adaptive tuning.
     *
     * @param ptr native engine pointer
     * @param enabled true to enable
     */
    private static native void nativeSetEnabled(long ptr, boolean enabled);

    /**
     * Check if adaptive tuning is enabled.
     *
     * @param ptr native engine pointer
     * @return true if enabled
     */
    private static native boolean nativeIsEnabled(long ptr);

    /**
     * Get current gap tolerance.
     *
     * @param ptr native engine pointer
     * @return gap tolerance in bytes
     */
    private static native long nativeGetGapTolerance(long ptr);

    /**
     * Get current max range size.
     *
     * @param ptr native engine pointer
     * @return max range size in bytes
     */
    private static native long nativeGetMaxRangeSize(long ptr);

    /**
     * Reset adaptive tuning history.
     *
     * @param ptr native engine pointer
     */
    private static native void nativeReset(long ptr);

    /**
     * Destroy native adaptive tuning engine and free resources.
     *
     * @param ptr native engine pointer
     */
    private static native void nativeDestroy(long ptr);
}
