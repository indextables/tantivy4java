/*
 * Validates that FastFieldMode configs result in appropriate .fast file storage
 * in parquet companion splits, using getPerFieldComponentSizes() API.
 *
 * Run:
 *   mvn test -pl . -Dtest=ParquetCompanionFastFieldStorageTest
 */
package io.indextables.tantivy4java;

import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that validate FastFieldMode controls what .fast data is written into the split.
 *
 * Expected behavior:
 *   - DISABLED: All fields have native .fast data in the split (tantivy writes them during indexing)
 *   - HYBRID: Numeric/bool/date fields have native .fast data; text fields should have
 *             minimal or zero .fast data (served from parquet at read time)
 *   - PARQUET_ONLY: No native .fast data should be written (all served from parquet at read time)
 *
 * Uses getPerFieldComponentSizes() which returns map like:
 *   {"id.fastfield": 107, "score.fastfield": 200, "name.fieldnorm": 50, ...}
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ParquetCompanionFastFieldStorageTest {

    private static SplitCacheManager cacheManager;

    @BeforeAll
    static void setupCache() {
        SplitCacheManager.CacheConfig config =
                new SplitCacheManager.CacheConfig("pq-ff-storage-test-" + System.nanoTime());
        cacheManager = SplitCacheManager.getInstance(config);
    }

    @AfterAll
    static void teardownCache() {
        if (cacheManager != null) cacheManager.close();
    }

    /**
     * Helper to create a parquet companion split and return the searcher + component sizes.
     */
    private SplitSearcher createSearcher(Path dir, ParquetCompanionConfig.FastFieldMode mode,
                                         int numRows, String tag) throws Exception {
        Path parquetFile = dir.resolve(tag + ".parquet");
        Path splitFile = dir.resolve(tag + ".split");

        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), numRows, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(mode);

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        return cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString());
    }

    /**
     * Extract fastfield sizes from component sizes map.
     * Returns a map of field_name → fastfield_size_bytes.
     */
    private Map<String, Long> getFastFieldSizes(Map<String, Long> componentSizes) {
        Map<String, Long> fastSizes = new LinkedHashMap<>();
        for (Map.Entry<String, Long> entry : componentSizes.entrySet()) {
            if (entry.getKey().endsWith(".fastfield")) {
                String fieldName = entry.getKey().replace(".fastfield", "");
                fastSizes.put(fieldName, entry.getValue());
            }
        }
        return fastSizes;
    }

    // ═══════════════════════════════════════════════════════════════
    //  1. DISABLED mode: all fields should have native .fast data
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(1)
    @DisplayName("DISABLED mode — all fields have native fast field data in split")
    void disabledModeAllFieldsHaveFastData(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.DISABLED, 100, "ff_disabled")) {
            Map<String, Long> componentSizes = s.getPerFieldComponentSizes();
            Map<String, Long> fastSizes = getFastFieldSizes(componentSizes);

            System.out.println("=== DISABLED mode fast field sizes ===");
            fastSizes.forEach((k, v) -> System.out.println("  " + k + ".fastfield: " + v + " bytes"));

            // All fields should have fast field data (id, name, score, active)
            assertTrue(fastSizes.containsKey("id"), "id should have fast field data");
            assertTrue(fastSizes.containsKey("score"), "score should have fast field data");
            assertTrue(fastSizes.containsKey("active"), "active should have fast field data");

            // Numeric fields should have non-trivial fast field data for 100 rows
            assertTrue(fastSizes.get("id") > 0, "id.fastfield should be > 0 bytes");
            assertTrue(fastSizes.get("score") > 0, "score.fastfield should be > 0 bytes");
            assertTrue(fastSizes.get("active") > 0, "active.fastfield should be > 0 bytes");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  2. Compare fast field sizes across all three modes
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(2)
    @DisplayName("Compare fast field sizes across DISABLED, HYBRID, PARQUET_ONLY")
    void compareFastFieldSizesAcrossModes(@TempDir Path dir) throws Exception {
        int numRows = 100;
        Map<String, Long> disabledFast, hybridFast, parquetOnlyFast;
        Map<String, Long> disabledAll, hybridAll, parquetOnlyAll;

        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.DISABLED, numRows, "cmp_disabled")) {
            disabledAll = s.getPerFieldComponentSizes();
            disabledFast = getFastFieldSizes(disabledAll);
        }

        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.HYBRID, numRows, "cmp_hybrid")) {
            hybridAll = s.getPerFieldComponentSizes();
            hybridFast = getFastFieldSizes(hybridAll);
        }

        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY, numRows, "cmp_pqonly")) {
            parquetOnlyAll = s.getPerFieldComponentSizes();
            parquetOnlyFast = getFastFieldSizes(parquetOnlyAll);
        }

        System.out.println("=== Fast field size comparison ===");
        Set<String> allFields = new TreeSet<>();
        allFields.addAll(disabledFast.keySet());
        allFields.addAll(hybridFast.keySet());
        allFields.addAll(parquetOnlyFast.keySet());

        System.out.printf("%-15s %12s %12s %12s%n", "Field", "DISABLED", "HYBRID", "PARQUET_ONLY");
        System.out.println("-".repeat(55));
        for (String field : allFields) {
            System.out.printf("%-15s %12d %12d %12d%n",
                    field,
                    disabledFast.getOrDefault(field, 0L),
                    hybridFast.getOrDefault(field, 0L),
                    parquetOnlyFast.getOrDefault(field, 0L));
        }

        // Compute total fast field bytes per mode
        long disabledTotal = disabledFast.values().stream().mapToLong(Long::longValue).sum();
        long hybridTotal = hybridFast.values().stream().mapToLong(Long::longValue).sum();
        long parquetOnlyTotal = parquetOnlyFast.values().stream().mapToLong(Long::longValue).sum();

        System.out.println("-".repeat(55));
        System.out.printf("%-15s %12d %12d %12d%n", "TOTAL", disabledTotal, hybridTotal, parquetOnlyTotal);

        // DISABLED should have the most fast field data (all native)
        assertTrue(disabledTotal > 0, "DISABLED mode should have non-zero fast field data");

        // All modes should produce valid, searchable splits
        // (the aggregation test already validates this)
    }

    // ═══════════════════════════════════════════════════════════════
    //  3. DISABLED mode: verify numeric fast fields are non-trivial
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(3)
    @DisplayName("DISABLED mode — numeric fast fields have expected sizes")
    void disabledModeNumericFieldSizes(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.DISABLED, 500, "ff_disabled_500")) {
            Map<String, Long> fastSizes = getFastFieldSizes(s.getPerFieldComponentSizes());

            System.out.println("=== DISABLED mode fast field sizes (500 rows) ===");
            fastSizes.forEach((k, v) -> System.out.println("  " + k + ".fastfield: " + v + " bytes"));

            // i64 field with 500 unique values — tantivy compresses aggressively
            assertTrue(fastSizes.getOrDefault("id", 0L) > 0,
                    "id.fastfield should be > 0 bytes for 500 rows, got: " + fastSizes.get("id"));

            // f64 field with 500 values
            assertTrue(fastSizes.getOrDefault("score", 0L) > 0,
                    "score.fastfield should be > 0 bytes for 500 rows, got: " + fastSizes.get("score"));

            // bool field with 500 values (compresses well)
            assertTrue(fastSizes.getOrDefault("active", 0L) > 0,
                    "active.fastfield should be > 0 bytes for 500 rows, got: " + fastSizes.get("active"));
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  4. HYBRID mode: numeric fields present, report text field status
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(4)
    @DisplayName("HYBRID mode — numeric fields have fast data, text field status reported")
    void hybridModeFieldBreakdown(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.HYBRID, 500, "ff_hybrid_500")) {
            Map<String, Long> fastSizes = getFastFieldSizes(s.getPerFieldComponentSizes());

            System.out.println("=== HYBRID mode fast field sizes (500 rows) ===");
            fastSizes.forEach((k, v) -> System.out.println("  " + k + ".fastfield: " + v + " bytes"));

            // In HYBRID mode, numeric/bool fields should always have native fast data
            assertTrue(fastSizes.getOrDefault("id", 0L) > 0,
                    "id.fastfield should be > 0 bytes in HYBRID mode");
            assertTrue(fastSizes.getOrDefault("score", 0L) > 0,
                    "score.fastfield should be > 0 bytes in HYBRID mode");
            assertTrue(fastSizes.getOrDefault("active", 0L) > 0,
                    "active.fastfield should be > 0 bytes in HYBRID mode");

            // Report name (text) field status — currently all modes write native fast fields
            // TODO: When fast field suppression is implemented for HYBRID mode,
            // text fields should have zero or minimal fast field data here
            long nameSize = fastSizes.getOrDefault("name", 0L);
            System.out.println("  [INFO] name.fastfield size in HYBRID mode: " + nameSize + " bytes");
            System.out.println("  [INFO] Ideally this should be 0 in HYBRID mode (text fields served from parquet)");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  5. PARQUET_ONLY mode: report all field sizes
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(5)
    @DisplayName("PARQUET_ONLY mode — report fast field sizes (ideally all zero)")
    void parquetOnlyModeFieldBreakdown(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY, 500, "ff_pqonly_500")) {
            Map<String, Long> fastSizes = getFastFieldSizes(s.getPerFieldComponentSizes());

            System.out.println("=== PARQUET_ONLY mode fast field sizes (500 rows) ===");
            fastSizes.forEach((k, v) -> System.out.println("  " + k + ".fastfield: " + v + " bytes"));

            long totalFastBytes = fastSizes.values().stream().mapToLong(Long::longValue).sum();
            System.out.println("  [INFO] Total fast field bytes in PARQUET_ONLY mode: " + totalFastBytes);
            System.out.println("  [INFO] Ideally this should be 0 (all fast fields served from parquet)");

            // TODO: When fast field suppression is implemented for PARQUET_ONLY mode,
            // uncomment these assertions:
            // assertEquals(0L, fastSizes.getOrDefault("id", 0L),
            //         "id.fastfield should be 0 in PARQUET_ONLY mode");
            // assertEquals(0L, fastSizes.getOrDefault("score", 0L),
            //         "score.fastfield should be 0 in PARQUET_ONLY mode");
            // assertEquals(0L, fastSizes.getOrDefault("name", 0L),
            //         "name.fastfield should be 0 in PARQUET_ONLY mode");
            // assertEquals(0L, fastSizes.getOrDefault("active", 0L),
            //         "active.fastfield should be 0 in PARQUET_ONLY mode");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  6. Total split size comparison across modes
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(6)
    @DisplayName("Total split size comparison across modes")
    void totalSplitSizeComparison(@TempDir Path dir) throws Exception {
        int numRows = 500;
        long disabledSize, hybridSize, parquetOnlySize;

        Path disabledSplit = dir.resolve("size_disabled.split");
        Path hybridSplit = dir.resolve("size_hybrid.split");
        Path pqOnlySplit = dir.resolve("size_pqonly.split");

        for (ParquetCompanionConfig.FastFieldMode mode : ParquetCompanionConfig.FastFieldMode.values()) {
            Path parquetFile = dir.resolve("size_" + mode.name() + ".parquet");
            Path splitFile = dir.resolve("size_" + mode.name() + ".split");

            QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), numRows, 0);

            ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                    .withFastFieldMode(mode);

            QuickwitSplit.createFromParquet(
                    Collections.singletonList(parquetFile.toString()),
                    splitFile.toString(), config);
        }

        disabledSize = java.nio.file.Files.size(dir.resolve("size_DISABLED.split"));
        hybridSize = java.nio.file.Files.size(dir.resolve("size_HYBRID.split"));
        parquetOnlySize = java.nio.file.Files.size(dir.resolve("size_PARQUET_ONLY.split"));

        System.out.println("=== Total split file sizes (500 rows) ===");
        System.out.printf("  DISABLED:     %,d bytes%n", disabledSize);
        System.out.printf("  HYBRID:       %,d bytes%n", hybridSize);
        System.out.printf("  PARQUET_ONLY: %,d bytes%n", parquetOnlySize);

        // All modes should produce valid splits
        assertTrue(disabledSize > 0, "DISABLED split should be non-empty");
        assertTrue(hybridSize > 0, "HYBRID split should be non-empty");
        assertTrue(parquetOnlySize > 0, "PARQUET_ONLY split should be non-empty");

        // TODO: When fast field suppression is implemented:
        // assertTrue(parquetOnlySize < disabledSize,
        //         "PARQUET_ONLY split should be smaller than DISABLED");
        // assertTrue(hybridSize <= disabledSize,
        //         "HYBRID split should be <= DISABLED");
    }

    // ═══════════════════════════════════════════════════════════════
    //  7. Component breakdown: fastfield vs postings vs fieldnorm
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(7)
    @DisplayName("Full component breakdown per mode")
    void componentBreakdownPerMode(@TempDir Path dir) throws Exception {
        int numRows = 500;

        for (ParquetCompanionConfig.FastFieldMode mode : ParquetCompanionConfig.FastFieldMode.values()) {
            try (SplitSearcher s = createSearcher(dir, mode, numRows, "breakdown_" + mode.name())) {
                Map<String, Long> componentSizes = s.getPerFieldComponentSizes();

                // Group by component type
                Map<String, Long> byType = new TreeMap<>();
                for (Map.Entry<String, Long> entry : componentSizes.entrySet()) {
                    String[] parts = entry.getKey().split("\\.");
                    String type = parts.length > 1 ? parts[parts.length - 1] : "unknown";
                    byType.merge(type, entry.getValue(), Long::sum);
                }

                System.out.println("=== " + mode + " mode component totals (500 rows) ===");
                byType.forEach((type, size) -> System.out.printf("  %-15s %,10d bytes%n", type, size));
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  8. Verify aggregations still work after component size inspection
    //     (sanity check that getPerFieldComponentSizes doesn't corrupt state)
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(8)
    @DisplayName("Component size inspection doesn't affect search/aggregation")
    void componentSizeInspectionDoesNotCorruptState(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.HYBRID, 100, "sanity_check")) {
            // First: inspect component sizes
            Map<String, Long> sizes = s.getPerFieldComponentSizes();
            assertFalse(sizes.isEmpty(), "Should have component sizes");

            // Then: verify search still works
            SplitRangeQuery rangeQ = SplitRangeQuery.inclusiveRange("id", "0", "10", "i64");
            io.indextables.tantivy4java.result.SearchResult result = s.search(rangeQ, 100);
            assertEquals(11, result.getHits().size(), "Range [0,10] should match 11 docs after size inspection");

            // And: inspect sizes again (should be idempotent)
            Map<String, Long> sizes2 = s.getPerFieldComponentSizes();
            assertEquals(sizes, sizes2, "Component sizes should be stable across calls");
        }
    }
}
