/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.SplitCacheManager.ComprehensiveCacheStats;
import io.indextables.tantivy4java.split.SplitCacheManager.CacheTypeStats;
import io.indextables.tantivy4java.split.merge.QuickwitSplit;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for field-specific prewarm functionality.
 *
 * These tests validate that calling preloadFields() for specific fields actually
 * triggers I/O to download the data, not just create lazy readers or read metadata.
 *
 * BACKGROUND:
 * Previously, field-specific prewarm for FASTFIELD and FIELDNORM was broken:
 * - FASTFIELD used sync Column readers that don't read data until iteration
 * - FIELDNORM used sync get_field() that returns a reader without reading
 *
 * These tests verify the fix using cache miss counts as a proxy for actual I/O:
 * - If preloadFields() triggers actual downloads, cache misses increase significantly
 * - If it only reads metadata, cache misses are minimal (< threshold)
 *
 * The tests compare cache misses from field-specific prewarm against expected minimums
 * based on actual index data size.
 */
public class FieldSpecificPrewarmRegressionTest {

    @TempDir
    Path tempDir;

    private static final int NUM_DOCUMENTS = 100;
    private static final Random random = new Random(42);

    // These will be populated from actual split metadata in setUp()
    // We validate that downloaded bytes are > 0 and <= component size
    private long actualFastFieldSize = 0;
    private long actualFieldNormSize = 0;
    private long actualPostingsSize = 0;  // .idx file
    private long actualTermSize = 0;
    private long actualPositionsSize = 0;

    private Path splitPath;
    private String splitUrl;
    private QuickwitSplit.SplitMetadata metadata;

    private static long testCounter = System.currentTimeMillis();

    @BeforeEach
    void setUp() throws Exception {
        // Use unique paths per test to avoid cache conflicts
        long unique = testCounter++;
        Path indexPath = tempDir.resolve("prewarm-regression-index-" + unique);
        splitPath = tempDir.resolve("prewarm-regression-" + unique + ".split");

        createTestIndex(indexPath);

        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "prewarm-regression-" + unique, "test-source", "test-node");
        metadata = QuickwitSplit.convertIndexFromPath(
            indexPath.toString(), splitPath.toString(), config);

        splitUrl = "file://" + splitPath.toAbsolutePath();
    }

    @Test
    @DisplayName("FASTFIELD field-specific prewarm triggers actual I/O")
    void testFastFieldSpecificPrewarmTriggersIO() throws Exception {
        System.out.println("=".repeat(80));
        System.out.println("FASTFIELD FIELD-SPECIFIC PREWARM REGRESSION TEST");
        System.out.println("=".repeat(80));
        System.out.println("Validates that preloadFields(FASTFIELD, 'score') downloads column data");
        System.out.println("Not just creates lazy Column readers");
        System.out.println();

        Path diskCachePath = tempDir.resolve("fastfield-regression-cache");
        Files.createDirectories(diskCachePath);

        // Use fresh cache to ensure cold start
        String cacheName = "fastfield-regression-" + System.currentTimeMillis();
        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath.toString())
            .withMaxDiskSize(100_000_000);

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig(cacheName)
            .withMaxCacheSize(10_000_000)
            .withTieredCache(tieredConfig);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata)) {

            // Get actual component size from split metadata
            SplitSearcher.SplitMetadata splitMeta = searcher.getSplitMetadata();
            long actualFastSize = getComponentSize(splitMeta, ".fast");
            System.out.println("Actual .fast file size from footer: " + actualFastSize + " bytes");

            // Get comprehensive storage stats before prewarm
            ComprehensiveCacheStats statsBefore = cacheManager.getComprehensiveCacheStats();
            CacheTypeStats byteRangeBefore = statsBefore.getByteRangeCache();
            CacheTypeStats fastFieldBefore = statsBefore.getFastFieldCache();

            System.out.println("\nBefore prewarm:");
            System.out.println("  ByteRangeCache: misses=" + byteRangeBefore.getMisses() +
                             ", size=" + byteRangeBefore.getSizeBytes() + " bytes");
            System.out.println("  FastFieldCache: misses=" + fastFieldBefore.getMisses() +
                             ", size=" + fastFieldBefore.getSizeBytes() + " bytes");

            // Call field-specific FASTFIELD prewarm
            System.out.println("\nCalling preloadFields(FASTFIELD, 'score')...");
            searcher.preloadFields(SplitSearcher.IndexComponent.FASTFIELD, "score").join();

            // Get storage stats after prewarm
            ComprehensiveCacheStats statsAfter = cacheManager.getComprehensiveCacheStats();
            CacheTypeStats byteRangeAfter = statsAfter.getByteRangeCache();
            CacheTypeStats fastFieldAfter = statsAfter.getFastFieldCache();

            long byteRangeSizeDelta = byteRangeAfter.getSizeBytes() - byteRangeBefore.getSizeBytes();
            long fastFieldSizeDelta = fastFieldAfter.getSizeBytes() - fastFieldBefore.getSizeBytes();
            long totalNewBytes = byteRangeSizeDelta + fastFieldSizeDelta;

            System.out.println("\nAfter prewarm:");
            System.out.println("  ByteRangeCache: +" + byteRangeSizeDelta + " bytes");
            System.out.println("  FastFieldCache: +" + fastFieldSizeDelta + " bytes");
            System.out.println("  Total downloaded: " + totalNewBytes + " bytes");
            System.out.println("  Actual .fast size: " + actualFastSize + " bytes");

            // REGRESSION CHECK: Must have downloaded actual data
            assertTrue(totalNewBytes > 0,
                "FASTFIELD prewarm must download > 0 bytes. Downloaded: " + totalNewBytes);
            assertTrue(totalNewBytes <= actualFastSize * 2, // Allow 2x for overhead/duplicates
                "FASTFIELD prewarm should not exceed 2x component size. Downloaded: " + totalNewBytes +
                ", component size: " + actualFastSize);

            System.out.println("\n✅ PASSED: FASTFIELD field-specific prewarm triggered actual I/O");
            System.out.println("   Downloaded " + totalNewBytes + " bytes (component size: " + actualFastSize + ")");
        }
    }

    /** Helper to get component size by file extension */
    private long getComponentSize(SplitSearcher.SplitMetadata meta, String extension) {
        if (meta == null) return 0;
        Map<String, Long> sizes = meta.getComponentSizes();
        if (sizes == null) return 0;
        for (Map.Entry<String, Long> entry : sizes.entrySet()) {
            if (entry.getKey().endsWith(extension)) {
                return entry.getValue();
            }
        }
        return 0;
    }

    @Test
    @DisplayName("FIELDNORM field-specific prewarm triggers actual I/O")
    void testFieldNormSpecificPrewarmTriggersIO() throws Exception {
        System.out.println("=".repeat(80));
        System.out.println("FIELDNORM FIELD-SPECIFIC PREWARM REGRESSION TEST");
        System.out.println("=".repeat(80));
        System.out.println("Validates that preloadFields(FIELDNORM, 'content') downloads norm data");
        System.out.println("Not just creates lazy readers");
        System.out.println();

        Path diskCachePath = tempDir.resolve("fieldnorm-regression-cache");
        Files.createDirectories(diskCachePath);

        String cacheName = "fieldnorm-regression-" + System.currentTimeMillis();
        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath.toString())
            .withMaxDiskSize(100_000_000);

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig(cacheName)
            .withMaxCacheSize(10_000_000)
            .withTieredCache(tieredConfig);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata)) {

            // Get actual component size from split metadata
            SplitSearcher.SplitMetadata splitMeta = searcher.getSplitMetadata();
            long actualFieldNormSize = getComponentSize(splitMeta, ".fieldnorm");
            System.out.println("Actual .fieldnorm file size from footer: " + actualFieldNormSize + " bytes");

            ComprehensiveCacheStats statsBefore = cacheManager.getComprehensiveCacheStats();
            CacheTypeStats byteRangeBefore = statsBefore.getByteRangeCache();

            System.out.println("\nBefore prewarm:");
            System.out.println("  ByteRangeCache: size=" + byteRangeBefore.getSizeBytes() + " bytes");

            System.out.println("\nCalling preloadFields(FIELDNORM, 'content')...");
            searcher.preloadFields(SplitSearcher.IndexComponent.FIELDNORM, "content").join();

            ComprehensiveCacheStats statsAfter = cacheManager.getComprehensiveCacheStats();
            CacheTypeStats byteRangeAfter = statsAfter.getByteRangeCache();

            long byteRangeSizeDelta = byteRangeAfter.getSizeBytes() - byteRangeBefore.getSizeBytes();

            System.out.println("\nAfter prewarm:");
            System.out.println("  ByteRangeCache: +" + byteRangeSizeDelta + " bytes");
            System.out.println("  Actual .fieldnorm size: " + actualFieldNormSize + " bytes");

            // REGRESSION CHECK: Must download actual data, bounded by component size
            assertTrue(byteRangeSizeDelta > 0,
                "FIELDNORM prewarm must download > 0 bytes. Downloaded: " + byteRangeSizeDelta);
            assertTrue(byteRangeSizeDelta <= actualFieldNormSize * 2,
                "FIELDNORM prewarm should not exceed 2x component size. Downloaded: " + byteRangeSizeDelta +
                ", component size: " + actualFieldNormSize);

            System.out.println("\n✅ PASSED: FIELDNORM field-specific prewarm triggered actual I/O");
            System.out.println("   Downloaded " + byteRangeSizeDelta + " bytes (component size: " + actualFieldNormSize + ")");
        }
    }

    @Test
    @DisplayName("POSTINGS field-specific prewarm triggers actual I/O")
    void testPostingsSpecificPrewarmTriggersIO() throws Exception {
        System.out.println("=".repeat(80));
        System.out.println("POSTINGS FIELD-SPECIFIC PREWARM REGRESSION TEST");
        System.out.println("=".repeat(80));
        System.out.println("Validates that preloadFields(POSTINGS, 'content') downloads posting lists");
        System.out.println();

        Path diskCachePath = tempDir.resolve("postings-regression-cache");
        Files.createDirectories(diskCachePath);

        String cacheName = "postings-regression-" + System.currentTimeMillis();
        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath.toString())
            .withMaxDiskSize(100_000_000);

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig(cacheName)
            .withMaxCacheSize(10_000_000)
            .withTieredCache(tieredConfig);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata)) {

            // Get actual component size from split metadata (.idx = postings)
            SplitSearcher.SplitMetadata splitMeta = searcher.getSplitMetadata();
            long actualPostingsSize = getComponentSize(splitMeta, ".idx");
            System.out.println("Actual .idx (postings) file size from footer: " + actualPostingsSize + " bytes");

            ComprehensiveCacheStats statsBefore = cacheManager.getComprehensiveCacheStats();
            CacheTypeStats byteRangeBefore = statsBefore.getByteRangeCache();

            System.out.println("\nBefore prewarm:");
            System.out.println("  ByteRangeCache: size=" + byteRangeBefore.getSizeBytes() + " bytes");

            System.out.println("\nCalling preloadFields(POSTINGS, 'content')...");
            searcher.preloadFields(SplitSearcher.IndexComponent.POSTINGS, "content").join();

            ComprehensiveCacheStats statsAfter = cacheManager.getComprehensiveCacheStats();
            CacheTypeStats byteRangeAfter = statsAfter.getByteRangeCache();

            long byteRangeSizeDelta = byteRangeAfter.getSizeBytes() - byteRangeBefore.getSizeBytes();

            System.out.println("\nAfter prewarm:");
            System.out.println("  ByteRangeCache: +" + byteRangeSizeDelta + " bytes");
            System.out.println("  Actual .idx size: " + actualPostingsSize + " bytes");

            // REGRESSION CHECK: Must download actual data, bounded by component size
            assertTrue(byteRangeSizeDelta > 0,
                "POSTINGS prewarm must download > 0 bytes. Downloaded: " + byteRangeSizeDelta);
            assertTrue(byteRangeSizeDelta <= actualPostingsSize * 2,
                "POSTINGS prewarm should not exceed 2x component size. Downloaded: " + byteRangeSizeDelta +
                ", component size: " + actualPostingsSize);

            System.out.println("\n✅ PASSED: POSTINGS field-specific prewarm triggered actual I/O");
            System.out.println("   Downloaded " + byteRangeSizeDelta + " bytes (component size: " + actualPostingsSize + ")");
        }
    }

    @Test
    @DisplayName("TERM field-specific prewarm triggers actual I/O")
    void testTermSpecificPrewarmTriggersIO() throws Exception {
        System.out.println("=".repeat(80));
        System.out.println("TERM FIELD-SPECIFIC PREWARM REGRESSION TEST");
        System.out.println("=".repeat(80));
        System.out.println("Validates that preloadFields(TERM, 'content') downloads term dictionary");
        System.out.println();

        Path diskCachePath = tempDir.resolve("term-regression-cache");
        Files.createDirectories(diskCachePath);

        String cacheName = "term-regression-" + System.currentTimeMillis();
        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath.toString())
            .withMaxDiskSize(100_000_000);

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig(cacheName)
            .withMaxCacheSize(10_000_000)
            .withTieredCache(tieredConfig);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata)) {

            // Get actual component size from split metadata
            SplitSearcher.SplitMetadata splitMeta = searcher.getSplitMetadata();
            long actualTermSize = getComponentSize(splitMeta, ".term");
            System.out.println("Actual .term file size from footer: " + actualTermSize + " bytes");

            ComprehensiveCacheStats statsBefore = cacheManager.getComprehensiveCacheStats();
            CacheTypeStats byteRangeBefore = statsBefore.getByteRangeCache();

            System.out.println("\nBefore prewarm:");
            System.out.println("  ByteRangeCache: size=" + byteRangeBefore.getSizeBytes() + " bytes");

            System.out.println("\nCalling preloadFields(TERM, 'content')...");
            searcher.preloadFields(SplitSearcher.IndexComponent.TERM, "content").join();

            ComprehensiveCacheStats statsAfter = cacheManager.getComprehensiveCacheStats();
            CacheTypeStats byteRangeAfter = statsAfter.getByteRangeCache();

            long byteRangeSizeDelta = byteRangeAfter.getSizeBytes() - byteRangeBefore.getSizeBytes();

            System.out.println("\nAfter prewarm:");
            System.out.println("  ByteRangeCache: +" + byteRangeSizeDelta + " bytes");
            System.out.println("  Actual .term size: " + actualTermSize + " bytes");

            // REGRESSION CHECK: Must download actual data, bounded by component size
            assertTrue(byteRangeSizeDelta > 0,
                "TERM prewarm must download > 0 bytes. Downloaded: " + byteRangeSizeDelta);
            assertTrue(byteRangeSizeDelta <= actualTermSize * 2,
                "TERM prewarm should not exceed 2x component size. Downloaded: " + byteRangeSizeDelta +
                ", component size: " + actualTermSize);

            System.out.println("\n✅ PASSED: TERM field-specific prewarm triggered actual I/O");
            System.out.println("   Downloaded " + byteRangeSizeDelta + " bytes (component size: " + actualTermSize + ")");
        }
    }

    @Test
    @DisplayName("POSITIONS field-specific prewarm triggers actual I/O")
    void testPositionsSpecificPrewarmTriggersIO() throws Exception {
        System.out.println("=".repeat(80));
        System.out.println("POSITIONS FIELD-SPECIFIC PREWARM REGRESSION TEST");
        System.out.println("=".repeat(80));
        System.out.println("Validates that preloadFields(POSITIONS, 'content') downloads position data");
        System.out.println();

        Path diskCachePath = tempDir.resolve("positions-regression-cache");
        Files.createDirectories(diskCachePath);

        String cacheName = "positions-regression-" + System.currentTimeMillis();
        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath.toString())
            .withMaxDiskSize(100_000_000);

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig(cacheName)
            .withMaxCacheSize(10_000_000)
            .withTieredCache(tieredConfig);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata)) {

            // Get actual component size from split metadata
            SplitSearcher.SplitMetadata splitMeta = searcher.getSplitMetadata();
            long actualPositionsSize = getComponentSize(splitMeta, ".pos");
            System.out.println("Actual .pos file size from footer: " + actualPositionsSize + " bytes");

            ComprehensiveCacheStats statsBefore = cacheManager.getComprehensiveCacheStats();
            CacheTypeStats byteRangeBefore = statsBefore.getByteRangeCache();

            System.out.println("\nBefore prewarm:");
            System.out.println("  ByteRangeCache: size=" + byteRangeBefore.getSizeBytes() + " bytes");

            System.out.println("\nCalling preloadFields(POSITIONS, 'content')...");
            searcher.preloadFields(SplitSearcher.IndexComponent.POSITIONS, "content").join();

            ComprehensiveCacheStats statsAfter = cacheManager.getComprehensiveCacheStats();
            CacheTypeStats byteRangeAfter = statsAfter.getByteRangeCache();

            long byteRangeSizeDelta = byteRangeAfter.getSizeBytes() - byteRangeBefore.getSizeBytes();

            System.out.println("\nAfter prewarm:");
            System.out.println("  ByteRangeCache: +" + byteRangeSizeDelta + " bytes");
            System.out.println("  Actual .pos size: " + actualPositionsSize + " bytes");

            // REGRESSION CHECK: Must download actual data
            // POSITIONS uses 3x bound because it may also read skip lists/index structures
            assertTrue(byteRangeSizeDelta > 0,
                "POSITIONS prewarm must download > 0 bytes. Downloaded: " + byteRangeSizeDelta);
            assertTrue(byteRangeSizeDelta <= actualPositionsSize * 3,
                "POSITIONS prewarm should not exceed 3x component size. Downloaded: " + byteRangeSizeDelta +
                ", component size: " + actualPositionsSize);

            System.out.println("\n✅ PASSED: POSITIONS field-specific prewarm triggered actual I/O");
            System.out.println("   Downloaded " + byteRangeSizeDelta + " bytes (component size: " + actualPositionsSize + ")");
        }
    }

    @Test
    @DisplayName("Validate split metadata exposes component sizes")
    void testSplitMetadataExposesComponentSizes() throws Exception {
        System.out.println("=".repeat(80));
        System.out.println("SPLIT METADATA COMPONENT SIZES TEST");
        System.out.println("=".repeat(80));
        System.out.println("Validates that getSplitMetadata() returns actual component sizes from footer");
        System.out.println();

        Path diskCachePath = tempDir.resolve("metadata-test-cache");
        Files.createDirectories(diskCachePath);

        String cacheName = "metadata-test-" + System.currentTimeMillis();
        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath.toString())
            .withMaxDiskSize(100_000_000);

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig(cacheName)
            .withMaxCacheSize(10_000_000)
            .withTieredCache(tieredConfig);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata)) {

            // Get split metadata with component sizes
            SplitSearcher.SplitMetadata splitMeta = searcher.getSplitMetadata();

            assertNotNull(splitMeta, "Split metadata should not be null");

            System.out.println("Split ID: " + splitMeta.getSplitId());
            System.out.println("Total size: " + splitMeta.getTotalSize() + " bytes");
            System.out.println("Hotcache size: " + splitMeta.getHotCacheSize() + " bytes");
            System.out.println("Num components: " + splitMeta.getNumComponents());
            System.out.println();
            System.out.println("Component sizes:");

            Map<String, Long> componentSizes = splitMeta.getComponentSizes();
            assertNotNull(componentSizes, "Component sizes should not be null");
            assertTrue(componentSizes.size() > 0, "Should have at least one component");

            long totalFromComponents = 0;
            for (Map.Entry<String, Long> entry : componentSizes.entrySet()) {
                System.out.println("  " + entry.getKey() + ": " + entry.getValue() + " bytes");
                totalFromComponents += entry.getValue();
            }

            System.out.println();
            System.out.println("Total from components: " + totalFromComponents + " bytes");

            // Validate totals match
            assertEquals(splitMeta.getTotalSize(), totalFromComponents,
                "Total size should equal sum of component sizes");

            System.out.println("\n✅ PASSED: Split metadata correctly exposes component sizes");
        }
    }

    @Test
    @DisplayName("Per-field component sizes API returns field-level breakdown")
    void testPerFieldComponentSizesAPI() throws Exception {
        System.out.println("=".repeat(80));
        System.out.println("PER-FIELD COMPONENT SIZES API TEST");
        System.out.println("=".repeat(80));
        System.out.println("Validates that getPerFieldComponentSizes() returns per-field breakdown");
        System.out.println();

        Path diskCachePath = tempDir.resolve("perfield-sizes-cache");
        Files.createDirectories(diskCachePath);

        String cacheName = "perfield-sizes-" + System.currentTimeMillis();
        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath.toString())
            .withMaxDiskSize(100_000_000);

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig(cacheName)
            .withMaxCacheSize(10_000_000)
            .withTieredCache(tieredConfig);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata)) {

            // Get per-field component sizes
            Map<String, Long> perFieldSizes = searcher.getPerFieldComponentSizes();

            System.out.println("Per-field component sizes:");
            for (Map.Entry<String, Long> entry : perFieldSizes.entrySet()) {
                System.out.println("  " + entry.getKey() + ": " + entry.getValue() + " bytes");
            }

            // Validate we have some per-field sizes
            assertTrue(perFieldSizes.size() > 0, "Should have at least one per-field component size");

            // Check for expected fields: score.fastfield (fast field), content.fieldnorm (text field)
            boolean hasScoreFastfield = perFieldSizes.containsKey("score.fastfield");
            boolean hasContentFieldnorm = perFieldSizes.containsKey("content.fieldnorm");
            boolean hasTitleFieldnorm = perFieldSizes.containsKey("title.fieldnorm");
            boolean hasIdFastfield = perFieldSizes.containsKey("id.fastfield");

            System.out.println();
            System.out.println("Expected fields present:");
            System.out.println("  score.fastfield: " + hasScoreFastfield);
            System.out.println("  id.fastfield: " + hasIdFastfield);
            System.out.println("  content.fieldnorm: " + hasContentFieldnorm);
            System.out.println("  title.fieldnorm: " + hasTitleFieldnorm);

            // At least one should be present
            assertTrue(hasScoreFastfield || hasIdFastfield || hasContentFieldnorm || hasTitleFieldnorm,
                "Should have at least one expected per-field component");

            System.out.println("\n✅ PASSED: Per-field component sizes API works correctly");
        }
    }

    @Test
    @DisplayName("All field-specific prewarm components in sequence")
    void testAllFieldSpecificPrewarmSequence() throws Exception {
        System.out.println("=".repeat(80));
        System.out.println("ALL FIELD-SPECIFIC PREWARM COMPONENTS SEQUENCE TEST");
        System.out.println("=".repeat(80));
        System.out.println("Validates each component type downloads data when called in sequence");
        System.out.println();

        Path diskCachePath = tempDir.resolve("all-components-cache");
        Files.createDirectories(diskCachePath);

        String cacheName = "all-components-" + System.currentTimeMillis();
        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath.toString())
            .withMaxDiskSize(100_000_000);

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig(cacheName)
            .withMaxCacheSize(10_000_000)
            .withTieredCache(tieredConfig);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata)) {

            // Test each component type
            var components = new SplitSearcher.IndexComponent[] {
                SplitSearcher.IndexComponent.TERM,
                SplitSearcher.IndexComponent.POSTINGS,
                SplitSearcher.IndexComponent.POSITIONS,
                SplitSearcher.IndexComponent.FASTFIELD,
                SplitSearcher.IndexComponent.FIELDNORM
            };

            String[] fieldNames = { "content", "content", "content", "score", "content" };
            // Map components to their file extensions
            String[] extensions = { ".term", ".idx", ".pos", ".fast", ".fieldnorm" };
            // Multipliers: POSITIONS uses 3x because it reads skip lists too
            int[] multipliers = { 2, 2, 3, 2, 2 };

            // Get actual component sizes from split metadata
            SplitSearcher.SplitMetadata splitMeta = searcher.getSplitMetadata();

            int passCount = 0;
            for (int i = 0; i < components.length; i++) {
                var component = components[i];
                String fieldName = fieldNames[i];
                String ext = extensions[i];
                int multiplier = multipliers[i];
                long actualSize = getComponentSize(splitMeta, ext);

                ComprehensiveCacheStats statsBefore = cacheManager.getComprehensiveCacheStats();
                long bytesBefore = statsBefore.getByteRangeCache().getSizeBytes() +
                                  statsBefore.getFastFieldCache().getSizeBytes();

                System.out.println("\nTesting " + component + " for field '" + fieldName + "'...");
                System.out.println("  Actual component size (" + ext + "): " + actualSize + " bytes");
                searcher.preloadFields(component, fieldName).join();

                ComprehensiveCacheStats statsAfter = cacheManager.getComprehensiveCacheStats();
                long bytesAfter = statsAfter.getByteRangeCache().getSizeBytes() +
                                 statsAfter.getFastFieldCache().getSizeBytes();
                long newBytes = bytesAfter - bytesBefore;

                // Validate: must download > 0 bytes and within multiplier bound
                boolean passed = newBytes > 0 && (actualSize == 0 || newBytes <= actualSize * multiplier);
                System.out.println("  Downloaded: " + newBytes + " bytes " +
                                 (passed ? "✅" : "❌"));

                if (passed) passCount++;
            }

            System.out.println("\n" + "=".repeat(80));
            System.out.println("RESULTS: " + passCount + "/" + components.length + " components passed");
            System.out.println("=".repeat(80));

            assertEquals(components.length, passCount,
                "All field-specific prewarm components must trigger actual I/O");
        }
    }

    @Test
    @DisplayName("Exact size matching and second prewarm downloads zero bytes")
    void testExactSizeMatchingAndSecondPrewarmZeroBytes() throws Exception {
        System.out.println("=".repeat(80));
        System.out.println("EXACT SIZE MATCHING AND SECOND PREWARM VALIDATION");
        System.out.println("=".repeat(80));
        System.out.println("Validates: 1) Downloads match per-field API sizes exactly");
        System.out.println("           2) Second prewarm downloads zero bytes (cache hit)");
        System.out.println();

        Path diskCachePath = tempDir.resolve("exact-match-cache");
        Files.createDirectories(diskCachePath);

        String cacheName = "exact-match-" + System.currentTimeMillis();
        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath.toString())
            .withMaxDiskSize(100_000_000);

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig(cacheName)
            .withMaxCacheSize(10_000_000)
            .withTieredCache(tieredConfig);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata)) {

            // Get per-field component sizes from API
            Map<String, Long> perFieldSizes = searcher.getPerFieldComponentSizes();
            System.out.println("Per-field component sizes from API:");
            for (Map.Entry<String, Long> e : perFieldSizes.entrySet()) {
                System.out.println("  " + e.getKey() + ": " + e.getValue() + " bytes");
            }

            int passCount = 0;
            int totalTests = 0;

            // Test FASTFIELD fields with exact matching
            System.out.println("\n--- FASTFIELD (exact matching) ---");
            for (String field : new String[]{"score", "id"}) {
                String key = field + ".fastfield";
                long apiSize = perFieldSizes.getOrDefault(key, 0L);

                // First prewarm: should download exactly apiSize bytes
                SplitCacheManager.resetStorageDownloadMetrics();
                searcher.preloadFields(SplitSearcher.IndexComponent.FASTFIELD, field).join();
                long firstDownload = SplitCacheManager.getStorageDownloadMetrics().getTotalBytes();

                // Second prewarm: should download 0 bytes (cache hit)
                SplitCacheManager.resetStorageDownloadMetrics();
                searcher.preloadFields(SplitSearcher.IndexComponent.FASTFIELD, field).join();
                long secondDownload = SplitCacheManager.getStorageDownloadMetrics().getTotalBytes();

                boolean firstMatch = (firstDownload == apiSize);
                boolean secondZero = (secondDownload == 0);
                boolean passed = firstMatch && secondZero;

                System.out.println("  " + key + ": API=" + apiSize + ", 1st=" + firstDownload +
                    ", 2nd=" + secondDownload + " -> " + (passed ? "PASS" : "FAIL"));

                totalTests++;
                if (passed) passCount++;
                assertTrue(firstMatch, key + " first prewarm should download exactly " + apiSize + " bytes, got " + firstDownload);
                assertEquals(0, secondDownload, key + " second prewarm should download 0 bytes");
            }

            // Test FIELDNORM fields with exact matching
            System.out.println("\n--- FIELDNORM (exact matching) ---");
            for (String field : new String[]{"content", "title"}) {
                String key = field + ".fieldnorm";
                long apiSize = perFieldSizes.getOrDefault(key, 0L);

                // First prewarm
                SplitCacheManager.resetStorageDownloadMetrics();
                searcher.preloadFields(SplitSearcher.IndexComponent.FIELDNORM, field).join();
                long firstDownload = SplitCacheManager.getStorageDownloadMetrics().getTotalBytes();

                // Second prewarm
                SplitCacheManager.resetStorageDownloadMetrics();
                searcher.preloadFields(SplitSearcher.IndexComponent.FIELDNORM, field).join();
                long secondDownload = SplitCacheManager.getStorageDownloadMetrics().getTotalBytes();

                boolean firstMatch = (firstDownload == apiSize);
                boolean secondZero = (secondDownload == 0);
                boolean passed = firstMatch && secondZero;

                System.out.println("  " + key + ": API=" + apiSize + ", 1st=" + firstDownload +
                    ", 2nd=" + secondDownload + " -> " + (passed ? "PASS" : "FAIL"));

                totalTests++;
                if (passed) passCount++;
                assertTrue(firstMatch, key + " first prewarm should download exactly " + apiSize + " bytes, got " + firstDownload);
                assertEquals(0, secondDownload, key + " second prewarm should download 0 bytes");
            }

            // Test TERM, POSTINGS, POSITIONS (per-field sizes not available, validate >0 and second=0)
            System.out.println("\n--- TERM/POSTINGS/POSITIONS (>0 first call, =0 second call) ---");
            var segmentComponents = new Object[][] {
                { SplitSearcher.IndexComponent.TERM, "content" },
                { SplitSearcher.IndexComponent.TERM, "title" },
                { SplitSearcher.IndexComponent.POSTINGS, "content" },
                { SplitSearcher.IndexComponent.POSTINGS, "title" },
                { SplitSearcher.IndexComponent.POSITIONS, "content" },
                { SplitSearcher.IndexComponent.POSITIONS, "title" }
            };

            for (Object[] pair : segmentComponents) {
                SplitSearcher.IndexComponent comp = (SplitSearcher.IndexComponent) pair[0];
                String field = (String) pair[1];
                String key = field + "." + comp.name().toLowerCase();

                // First prewarm
                SplitCacheManager.resetStorageDownloadMetrics();
                searcher.preloadFields(comp, field).join();
                long firstDownload = SplitCacheManager.getStorageDownloadMetrics().getTotalBytes();

                // Second prewarm
                SplitCacheManager.resetStorageDownloadMetrics();
                searcher.preloadFields(comp, field).join();
                long secondDownload = SplitCacheManager.getStorageDownloadMetrics().getTotalBytes();

                boolean firstPositive = (firstDownload > 0);
                boolean secondZero = (secondDownload == 0);
                boolean passed = firstPositive && secondZero;

                System.out.println("  " + key + ": 1st=" + firstDownload + ", 2nd=" + secondDownload +
                    " -> " + (passed ? "PASS" : "FAIL"));

                totalTests++;
                if (passed) passCount++;
                assertTrue(firstPositive, key + " first prewarm should download >0 bytes, got " + firstDownload);
                assertEquals(0, secondDownload, key + " second prewarm should download 0 bytes");
            }

            System.out.println("\n" + "=".repeat(80));
            System.out.println("RESULTS: " + passCount + "/" + totalTests + " tests passed");
            System.out.println("=".repeat(80));
            assertEquals(totalTests, passCount, "All tests must pass");
        }
    }

    private void createTestIndex(Path indexPath) throws Exception {
        try (SchemaBuilder schemaBuilder = new SchemaBuilder()) {
            // Text fields with positions (for POSITIONS prewarm)
            schemaBuilder.addTextField("content", true, false, "default", "position");
            schemaBuilder.addTextField("title", true, false, "default", "position");

            // Numeric fields with FAST enabled (for FASTFIELD prewarm)
            schemaBuilder.addIntegerField("id", true, true, true);
            schemaBuilder.addIntegerField("score", true, true, true);

            try (Schema schema = schemaBuilder.build()) {
                try (Index index = new Index(schema, indexPath.toString(), false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        for (int i = 0; i < NUM_DOCUMENTS; i++) {
                            try (Document doc = new Document()) {
                                // Add varied content to create meaningful term dictionaries
                                doc.addText("content",
                                    "Document " + i + " contains searchable text about topic" + (i % 10) +
                                    " with various words like alpha beta gamma delta epsilon");
                                doc.addText("title", "Title " + i);
                                doc.addInteger("id", i);
                                doc.addInteger("score", random.nextInt(100));
                                writer.addDocument(doc);
                            }
                        }

                        writer.commit();
                    }
                    index.reload();
                }
            }
        }
    }
}
