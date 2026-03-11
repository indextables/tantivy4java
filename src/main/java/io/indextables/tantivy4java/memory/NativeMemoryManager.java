package io.indextables.tantivy4java.memory;

import java.util.Collections;
import java.util.Map;

/**
 * Global configuration and monitoring for native memory management.
 *
 * <p>Call {@link #setAccountant(NativeMemoryAccountant)} once before any native
 * operations to enable JVM-coordinated memory tracking. If not called, the native
 * layer defaults to unlimited (untracked) mode.
 *
 * <p>Example usage:
 * <pre>{@code
 * // At application startup (before any Index/SplitSearcher use)
 * NativeMemoryManager.setAccountant(new SparkMemoryAccountant(taskMemoryManager));
 *
 * // Monitor memory usage
 * NativeMemoryStats stats = NativeMemoryManager.getStats();
 * System.out.println("Native memory used: " + stats.getUsedBytes());
 * System.out.println("Peak usage: " + stats.getPeakBytes());
 * stats.getCategoryBreakdown().forEach((cat, bytes) ->
 *     System.out.println("  " + cat + ": " + bytes + " bytes"));
 * }</pre>
 */
public class NativeMemoryManager {

    static {
        // Ensure the native library is loaded
        try {
            System.loadLibrary("tantivy4java");
        } catch (UnsatisfiedLinkError e) {
            // Library may already be loaded by another class
        }
    }

    private NativeMemoryManager() {
        // Static utility class
    }

    /**
     * Set the global memory accountant for native allocations.
     *
     * <p>Must be called before any Index, SplitSearcher, or SplitCacheManager use.
     * Can only be called once; subsequent calls return false.
     *
     * @param accountant the memory accountant to use
     * @return true if set successfully, false if already configured
     */
    public static boolean setAccountant(NativeMemoryAccountant accountant) {
        return setAccountant(accountant, 0.90, 0.25, 64 * 1024 * 1024, 64 * 1024 * 1024);
    }

    /**
     * Set the global memory accountant with custom watermark configuration.
     *
     * @param accountant the memory accountant to use
     * @param highWatermark acquire more from JVM when usage exceeds this fraction of grant (default 0.90)
     * @param lowWatermark release excess to JVM when usage drops below this fraction (default 0.25)
     * @param acquireIncrementBytes minimum JNI acquire chunk size in bytes (default 64MB)
     * @param minReleaseBytes minimum amount to release back in bytes (default 64MB)
     * @return true if set successfully, false if already configured
     */
    public static boolean setAccountant(
            NativeMemoryAccountant accountant,
            double highWatermark,
            double lowWatermark,
            long acquireIncrementBytes,
            long minReleaseBytes) {
        if (accountant == null) {
            throw new IllegalArgumentException("accountant must not be null");
        }
        return nativeSetAccountant(accountant, highWatermark, lowWatermark,
                acquireIncrementBytes, minReleaseBytes);
    }

    /**
     * Check if a custom memory accountant has been configured.
     */
    public static boolean isConfigured() {
        return nativeIsConfigured();
    }

    /**
     * Reset the peak usage counter to current usage.
     *
     * <p>Useful for monitoring windows — call at the start of each window to
     * track per-window peak usage.
     *
     * @return the old peak value in bytes
     */
    public static long resetPeak() {
        return nativeResetPeak();
    }

    /**
     * Get current native memory statistics.
     */
    public static NativeMemoryStats getStats() {
        long used = nativeGetUsedBytes();
        long peak = nativeGetPeakBytes();
        long granted = nativeGetGrantedBytes();
        Map<String, Long> breakdown = nativeGetCategoryBreakdown();
        if (breakdown == null) {
            breakdown = Collections.emptyMap();
        }
        return new NativeMemoryStats(used, peak, granted, breakdown);
    }

    // Native methods
    private static native boolean nativeSetAccountant(
            Object accountant, double highWatermark, double lowWatermark,
            long acquireIncrementBytes, long minReleaseBytes);

    private static native long nativeGetUsedBytes();
    private static native long nativeGetPeakBytes();
    private static native long nativeGetGrantedBytes();
    private static native long nativeResetPeak();
    private static native boolean nativeIsConfigured();
    private static native Map<String, Long> nativeGetCategoryBreakdown();
}
