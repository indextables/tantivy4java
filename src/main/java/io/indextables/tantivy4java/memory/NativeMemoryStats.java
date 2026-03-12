package io.indextables.tantivy4java.memory;

import java.util.Collections;
import java.util.Map;

/**
 * Snapshot of native memory pool statistics.
 *
 * <p>Categories tracked:
 * <ul>
 *   <li>{@code index_writer} — IndexWriter heap budget</li>
 *   <li>{@code merge} — In-process merge operations (3x heap for copies + mmaps)</li>
 *   <li>{@code l1_cache} — L1 ByteRangeCache memory</li>
 *   <li>{@code l2_write_queue} — L2 disk cache write queue buffer</li>
 *   <li>{@code arrow_ffi} — Arrow RecordBatch FFI exports</li>
 *   <li>{@code search_results} — Search result pre-allocated arenas</li>
 *   <li>{@code parquet_transcode} — Parquet fast field transcoding buffers</li>
 * </ul>
 */
public class NativeMemoryStats {

    private final long usedBytes;
    private final long peakBytes;
    private final long grantedBytes;
    private final Map<String, Long> categoryBreakdown;
    private final Map<String, Long> categoryPeakBreakdown;

    NativeMemoryStats(long usedBytes, long peakBytes, long grantedBytes,
                      Map<String, Long> categoryBreakdown,
                      Map<String, Long> categoryPeakBreakdown) {
        this.usedBytes = usedBytes;
        this.peakBytes = peakBytes;
        this.grantedBytes = grantedBytes;
        this.categoryBreakdown = Collections.unmodifiableMap(categoryBreakdown);
        this.categoryPeakBreakdown = Collections.unmodifiableMap(categoryPeakBreakdown);
    }

    // Backward-compatible constructor
    NativeMemoryStats(long usedBytes, long peakBytes, long grantedBytes,
                      Map<String, Long> categoryBreakdown) {
        this(usedBytes, peakBytes, grantedBytes, categoryBreakdown, Collections.emptyMap());
    }

    /** Current total bytes reserved by native code. */
    public long getUsedBytes() {
        return usedBytes;
    }

    /** Peak bytes observed since pool creation. */
    public long getPeakBytes() {
        return peakBytes;
    }

    /**
     * Total bytes granted by the external memory manager.
     * Returns -1 if using unlimited (untracked) mode.
     */
    public long getGrantedBytes() {
        return grantedBytes;
    }

    /** Per-category current memory breakdown (only non-zero categories). */
    public Map<String, Long> getCategoryBreakdown() {
        return categoryBreakdown;
    }

    /**
     * Per-category peak memory breakdown.
     *
     * <p>Returns the maximum bytes each category has ever held, even if
     * currently zero. Useful for post-hoc analysis when all reservations
     * have been released by the time this is called.
     */
    public Map<String, Long> getCategoryPeakBreakdown() {
        return categoryPeakBreakdown;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("NativeMemoryStats{");
        sb.append("used=").append(formatBytes(usedBytes));
        sb.append(", peak=").append(formatBytes(peakBytes));
        sb.append(", granted=").append(grantedBytes < 0 ? "unlimited" : formatBytes(grantedBytes));
        if (!categoryBreakdown.isEmpty()) {
            sb.append(", categories={");
            boolean first = true;
            for (Map.Entry<String, Long> entry : categoryBreakdown.entrySet()) {
                if (!first) sb.append(", ");
                sb.append(entry.getKey()).append("=").append(formatBytes(entry.getValue()));
                first = false;
            }
            sb.append("}");
        }
        sb.append("}");
        return sb.toString();
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + "B";
        if (bytes < 1024 * 1024) return String.format("%.1fKB", bytes / 1024.0);
        if (bytes < 1024L * 1024 * 1024) return String.format("%.1fMB", bytes / (1024.0 * 1024));
        return String.format("%.1fGB", bytes / (1024.0 * 1024 * 1024));
    }
}
