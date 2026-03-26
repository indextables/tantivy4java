package io.indextables.tantivy4java.split;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Near-zero-overhead profiler for the native FFI read path.
 *
 * <p>When disabled (~1ns per instrumented section), there is no measurable impact.
 * When enabled (~60ns per section), counters track invocation count, total/min/max
 * nanoseconds for each instrumented code section.
 *
 * <h3>Usage:</h3>
 * <pre>{@code
 * FfiProfiler.enable();  // auto-resets counters
 *
 * // ... run workload ...
 *
 * Map<String, FfiProfiler.ProfileEntry> profile = FfiProfiler.snapshot();
 * profile.values().forEach(System.out::println);
 *
 * Map<String, Long> cacheStats = FfiProfiler.cacheCounters();
 * cacheStats.forEach((k, v) -> System.out.println(k + ": " + v));
 *
 * FfiProfiler.disable();
 * }</pre>
 */
public class FfiProfiler {

    static {
        // Trigger Tantivy's static initializer which loads the native library
        io.indextables.tantivy4java.core.Tantivy.initialize();
    }

    // ── Native methods ──────────────────────────────────────────────

    private static native void nativeProfilerEnable();
    private static native void nativeProfilerDisable();
    private static native boolean nativeProfilerIsEnabled();
    private static native long[] nativeProfilerSnapshot();
    private static native long[] nativeProfilerReset();
    private static native long[] nativeProfilerCacheSnapshot();
    private static native long[] nativeProfilerCacheReset();

    // ── Section name table (must match Rust Section enum order) ─────

    private static final String[] SECTION_NAMES = {
        // Search flow (10)
        "jni_string_extract",
        "query_convert",
        "wildcard_analysis",
        "query_rewrite_hash",
        "query_rewrite_string_idx",
        "ensure_fast_fields",
        "doc_mapper_create",
        "permit_acquire",
        "leaf_search",
        "result_creation",
        // Aggregation flow (6)
        "agg_java_to_json",
        "agg_rewrite_hash",
        "agg_ensure_fast_fields",
        "agg_leaf_search",
        "agg_hash_touchup",
        "agg_result_creation",
        // Tantivy store doc retrieval (7)
        "doc_batch_jni_extract",
        "doc_batch_sort",
        "doc_batch_range_consolidate",
        "doc_batch_prefetch",
        "doc_batch_doc_async",
        "doc_batch_reorder",
        "doc_batch_create_java_objects",
        // Parquet companion doc retrieval (6)
        "pq_doc_jni_extract",
        "pq_doc_ensure_pq_fields",
        "pq_doc_resolve_locations",
        "pq_doc_group_by_file",
        "pq_doc_parquet_read",
        "pq_doc_jni_copy",
        // Single doc retrieval (3)
        "single_doc_cache_lookup",
        "single_doc_fetch",
        "single_doc_cache_miss_open",
        // Prewarm (1)
        "prewarm_components",
        // Parquet companion transcode pipeline (7)
        "pc_field_extraction",
        "pc_l2_cache_check",
        "pc_transcode_str",
        "pc_transcode_non_str",
        "pc_merge_columnars",
        "pc_native_fast_read",
        "pc_l2_cache_write",
        // Parquet companion hash touchup (3)
        "pc_hash_scan_fast_field",
        "pc_hash_load_pq_columns",
        "pc_hash_parquet_read",
        // Parquet doc retrieval internals (5)
        "pc_doc_create_reader",
        "pc_doc_row_group_filter",
        "pc_doc_row_selection",
        "pc_doc_stream_batches",
        "pc_doc_serialize_tant",
        // Streaming retrieval (3)
        "stream_setup",
        "stream_batch_read",
        "stream_batch_next",
        // Arrow FFI export (3)
        "arrow_ffi_resolve",
        "arrow_ffi_batch_read",
        "arrow_ffi_export",
    };

    private static final String[] CACHE_COUNTER_NAMES = {
        "byte_range_hit",
        "byte_range_miss",
        "l2_disk_hit",
        "l2_disk_miss",
        "searcher_hit",
        "searcher_miss",
        "parquet_metadata_hit",
        "parquet_metadata_miss",
        "pq_column_hit",
        "pq_column_miss",
    };

    private static final int VALUES_PER_SECTION = 4; // count, totalNanos, minNanos, maxNanos

    // ── Profile entry ───────────────────────────────────────────────

    /**
     * Profiling data for a single instrumented code section.
     */
    public static class ProfileEntry {
        private final String section;
        private final long count;
        private final long totalNanos;
        private final long minNanos;
        private final long maxNanos;

        ProfileEntry(String section, long count, long totalNanos, long minNanos, long maxNanos) {
            this.section = section;
            this.count = count;
            this.totalNanos = totalNanos;
            this.minNanos = minNanos;
            this.maxNanos = maxNanos;
        }

        public String getSection()   { return section; }
        public long getCount()       { return count; }
        public long getTotalNanos()  { return totalNanos; }
        public long getMinNanos()    { return minNanos; }
        public long getMaxNanos()    { return maxNanos; }

        public double totalMillis()  { return totalNanos / 1_000_000.0; }
        public double avgMicros()    { return count > 0 ? totalNanos / (double) count / 1_000.0 : 0.0; }
        public double minMicros()    { return minNanos / 1_000.0; }
        public double maxMicros()    { return maxNanos / 1_000.0; }

        @Override
        public String toString() {
            return String.format("%-30s  calls=%-8d  total=%8.2fms  avg=%8.2fµs  min=%8.2fµs  max=%8.2fµs",
                section, count, totalMillis(), avgMicros(), minMicros(), maxMicros());
        }
    }

    // ── Public API ──────────────────────────────────────────────────

    /**
     * Enable profiling. Automatically resets all counters for a clean start.
     */
    public static void enable() {
        nativeProfilerEnable();
    }

    /**
     * Disable profiling. Counters are preserved and can still be read.
     */
    public static void disable() {
        nativeProfilerDisable();
    }

    /**
     * Check if profiling is currently enabled.
     */
    public static boolean isEnabled() {
        return nativeProfilerIsEnabled();
    }

    /**
     * Snapshot current section counters without resetting.
     * Returns a map from section name to ProfileEntry. Sections with count=0 are omitted.
     * Map iteration order follows the logical flow: search → aggregation → doc retrieval → parquet companion.
     */
    public static Map<String, ProfileEntry> snapshot() {
        return decodeSectionArray(nativeProfilerSnapshot());
    }

    /**
     * Reset section counters and return pre-reset values.
     * Sections with count=0 are omitted.
     */
    public static Map<String, ProfileEntry> reset() {
        return decodeSectionArray(nativeProfilerReset());
    }

    /**
     * Snapshot cache hit/miss counters without resetting.
     * Returns a map from counter name to value. Counters with value=0 are omitted.
     */
    public static Map<String, Long> cacheCounters() {
        return decodeCacheArray(nativeProfilerCacheSnapshot());
    }

    /**
     * Reset cache counters and return pre-reset values.
     */
    public static Map<String, Long> resetCacheCounters() {
        return decodeCacheArray(nativeProfilerCacheReset());
    }

    // ── Decoding ────────────────────────────────────────────────────

    private static Map<String, ProfileEntry> decodeSectionArray(long[] raw) {
        if (raw == null) {
            return Collections.emptyMap();
        }
        Map<String, ProfileEntry> map = new LinkedHashMap<>();
        for (int i = 0; i < SECTION_NAMES.length && (i * VALUES_PER_SECTION + 3) < raw.length; i++) {
            long count     = raw[i * VALUES_PER_SECTION];
            long nanos     = raw[i * VALUES_PER_SECTION + 1];
            long minNanos  = raw[i * VALUES_PER_SECTION + 2];
            long maxNanos  = raw[i * VALUES_PER_SECTION + 3];
            if (count > 0) {
                map.put(SECTION_NAMES[i],
                    new ProfileEntry(SECTION_NAMES[i], count, nanos, minNanos, maxNanos));
            }
        }
        return map;
    }

    private static Map<String, Long> decodeCacheArray(long[] raw) {
        if (raw == null) {
            return Collections.emptyMap();
        }
        Map<String, Long> map = new LinkedHashMap<>();
        for (int i = 0; i < CACHE_COUNTER_NAMES.length && i < raw.length; i++) {
            if (raw[i] > 0) {
                map.put(CACHE_COUNTER_NAMES[i], raw[i]);
            }
        }
        return map;
    }
}
