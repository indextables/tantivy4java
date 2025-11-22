package io.indextables.tantivy4java.split;

/**
 * Statistics snapshot for adaptive tuning performance monitoring.
 *
 * <p>Provides insights into how adaptive tuning is performing and what
 * parameter adjustments have been made.
 *
 * <p>Priority 5: Adaptive Tuning - Statistics API
 *
 * <h3>Example Usage:</h3>
 * <pre>{@code
 * // Get adaptive tuning statistics (future JNI integration)
 * AdaptiveTuningStats stats = cacheManager.getAdaptiveTuningStats();
 *
 * System.out.println("Adaptive tuning enabled: " + stats.isEnabled());
 * System.out.println("Batches tracked: " + stats.getBatchesTracked());
 * System.out.println("Avg consolidation: " + stats.getAvgConsolidation() + "x");
 * System.out.println("Avg waste: " + (stats.getAvgWaste() * 100) + "%");
 * System.out.println("Current gap tolerance: " + (stats.getCurrentGapTolerance() / 1024) + "KB");
 * }</pre>
 */
public class AdaptiveTuningStats {

    private final boolean enabled;
    private final int batchesTracked;
    private final double avgConsolidation;
    private final double avgWaste;
    private final long currentGapTolerance;
    private final long currentMaxRangeSize;

    /**
     * Create adaptive tuning statistics snapshot.
     *
     * @param enabled whether adaptive tuning is enabled
     * @param batchesTracked number of batches in tracking history
     * @param avgConsolidation average consolidation ratio (documents per range)
     * @param avgWaste average waste ratio (0.0 - 1.0)
     * @param currentGapTolerance current gap tolerance in bytes
     * @param currentMaxRangeSize current max range size in bytes
     */
    public AdaptiveTuningStats(
            boolean enabled,
            int batchesTracked,
            double avgConsolidation,
            double avgWaste,
            long currentGapTolerance,
            long currentMaxRangeSize) {
        this.enabled = enabled;
        this.batchesTracked = batchesTracked;
        this.avgConsolidation = avgConsolidation;
        this.avgWaste = avgWaste;
        this.currentGapTolerance = currentGapTolerance;
        this.currentMaxRangeSize = currentMaxRangeSize;
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
     * Get number of batches tracked in history.
     *
     * @return batches tracked (max 20)
     */
    public int getBatchesTracked() {
        return batchesTracked;
    }

    /**
     * Get average consolidation ratio.
     *
     * <p>This represents how many documents are retrieved per consolidated range request.
     * Higher values indicate better consolidation (fewer S3 requests).
     *
     * @return average consolidation ratio (e.g., 15.5 means 15.5 documents per range)
     */
    public double getAvgConsolidation() {
        return avgConsolidation;
    }

    /**
     * Get average waste ratio.
     *
     * <p>This represents the fraction of fetched data that was not used due to gaps.
     * Lower values indicate more efficient bandwidth usage.
     *
     * @return average waste ratio (0.0 = no waste, 1.0 = 100% waste)
     */
    public double getAvgWaste() {
        return avgWaste;
    }

    /**
     * Get current gap tolerance in bytes.
     *
     * <p>This is the maximum gap that will be bridged when consolidating ranges.
     * Adaptive tuning adjusts this value between 64KB and 2MB.
     *
     * @return current gap tolerance in bytes
     */
    public long getCurrentGapTolerance() {
        return currentGapTolerance;
    }

    /**
     * Get current gap tolerance in KB.
     *
     * @return current gap tolerance in kilobytes
     */
    public long getCurrentGapToleranceKB() {
        return currentGapTolerance / 1024;
    }

    /**
     * Get current maximum range size in bytes.
     *
     * <p>This is the maximum size for a single consolidated range request.
     * Adaptive tuning adjusts this value between 2MB and 32MB.
     *
     * @return current max range size in bytes
     */
    public long getCurrentMaxRangeSize() {
        return currentMaxRangeSize;
    }

    /**
     * Get current maximum range size in MB.
     *
     * @return current max range size in megabytes
     */
    public long getCurrentMaxRangeSizeMB() {
        return currentMaxRangeSize / (1024 * 1024);
    }

    /**
     * Format statistics as human-readable string.
     *
     * @return formatted statistics string
     */
    public String toDetailedString() {
        return String.format(
            "AdaptiveTuningStats {\n" +
            "  enabled: %s\n" +
            "  batches tracked: %d\n" +
            "  avg consolidation: %.1fx\n" +
            "  avg waste: %.1f%%\n" +
            "  current gap tolerance: %dKB\n" +
            "  current max range size: %dMB\n" +
            "}",
            enabled,
            batchesTracked,
            avgConsolidation,
            avgWaste * 100.0,
            getCurrentGapToleranceKB(),
            getCurrentMaxRangeSizeMB()
        );
    }

    @Override
    public String toString() {
        return String.format(
            "AdaptiveTuningStats{enabled=%s, batches=%d, consolidation=%.1fx, waste=%.1f%%, gap=%dKB, maxRange=%dMB}",
            enabled,
            batchesTracked,
            avgConsolidation,
            avgWaste * 100.0,
            getCurrentGapToleranceKB(),
            getCurrentMaxRangeSizeMB()
        );
    }
}
