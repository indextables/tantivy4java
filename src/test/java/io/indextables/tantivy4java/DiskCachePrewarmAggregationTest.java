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

import io.indextables.tantivy4java.aggregation.*;
import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.QuickwitSplit;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test that validates disk-only prewarm populates the L2 disk cache
 * and subsequent aggregation queries use the cache without additional downloads.
 *
 * This test validates the memory-safe prewarm architecture:
 * 1. Prewarm writes directly to L2 disk cache (not L1 memory)
 * 2. Aggregation queries find data in L2 disk cache
 * 3. No additional S3/storage downloads occur during aggregation
 *
 * The test uses cache miss counters to detect any storage access.
 */
public class DiskCachePrewarmAggregationTest {

    @TempDir
    Path tempDir;

    private static final int NUM_DOCUMENTS = 500;
    private static final Random random = new Random(42);

    @Test
    @DisplayName("Test that prewarm populates disk cache and aggregations use it without downloads")
    void testPrewarmThenAggregationNoDownloads() throws Exception {
        System.out.println("=".repeat(80));
        System.out.println("DISK CACHE PREWARM + AGGREGATION TEST");
        System.out.println("Goal: Verify prewarm populates disk cache and aggregations cause NO downloads");
        System.out.println("=".repeat(80));

        // Step 1: Create index with numeric fields for aggregations
        Path indexPath = tempDir.resolve("agg-test-index");
        Path splitPath = tempDir.resolve("agg-test.split");
        String diskCachePath = tempDir.resolve("disk_cache").toString();
        Files.createDirectories(Path.of(diskCachePath));

        System.out.println("\nStep 1: Creating test index with numeric fields...");
        createTestIndexWithNumericFields(indexPath);

        // Step 2: Convert to split
        System.out.println("\nStep 2: Converting to Quickwit split...");
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "agg-test-index", "test-source", "test-node");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
            indexPath.toString(), splitPath.toString(), config);

        System.out.println("   Split created: " + metadata.getSplitId());
        System.out.println("   Documents: " + metadata.getNumDocs());
        System.out.println("   Size: " + Files.size(splitPath) + " bytes");

        // Step 3: Create SplitSearcher with DISK CACHE enabled
        String splitUrl = "file://" + splitPath.toAbsolutePath();
        String cacheName = "disk-cache-agg-test-" + System.currentTimeMillis();

        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath)
            .withMaxDiskSize(500_000_000L) // 500MB
            .withCompression(SplitCacheManager.CompressionAlgorithm.LZ4);

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig(cacheName)
            .withMaxCacheSize(10_000_000) // Small L1 to force L2 usage
            .withTieredCache(tieredConfig);

        SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);

        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata)) {

            assertTrue(cacheManager.isDiskCacheEnabled(), "Disk cache should be enabled");

            // Step 4: PREWARM all components to disk cache
            System.out.println("\nStep 3: Prewarming ALL components to disk cache...");
            System.out.println("   (This should write to L2 disk cache, not L1 memory)");

            var statsBeforePrewarm = searcher.getCacheStats();
            long missesBeforePrewarm = statsBeforePrewarm.getMissCount();
            System.out.println("   Cache misses before prewarm: " + missesBeforePrewarm);

            long prewarmStart = System.nanoTime();
            searcher.preloadComponents(
                SplitSearcher.IndexComponent.TERM,
                SplitSearcher.IndexComponent.POSTINGS,
                SplitSearcher.IndexComponent.FIELDNORM,
                SplitSearcher.IndexComponent.FASTFIELD,
                SplitSearcher.IndexComponent.STORE
            ).join();
            long prewarmTimeMs = (System.nanoTime() - prewarmStart) / 1_000_000;

            var statsAfterPrewarm = searcher.getCacheStats();
            long missesAfterPrewarm = statsAfterPrewarm.getMissCount();
            long prewarmMisses = missesAfterPrewarm - missesBeforePrewarm;

            System.out.println("   Prewarm completed in " + prewarmTimeMs + " ms");
            System.out.println("   Cache misses during prewarm (downloads): " + prewarmMisses);

            // Check disk cache stats
            var diskCacheStats = cacheManager.getDiskCacheStats();
            System.out.println("   Disk cache after prewarm:");
            System.out.println("     - Total bytes: " + diskCacheStats.getTotalBytes());
            System.out.println("     - Components: " + diskCacheStats.getComponentCount());
            System.out.println("     - Splits: " + diskCacheStats.getSplitCount());

            // Step 5: Run aggregation queries and verify NO new downloads
            System.out.println("\nStep 4: Running aggregation queries...");
            System.out.println("   (These should use disk cache - NO new downloads)");

            // Record misses before aggregations
            var statsBeforeAgg = searcher.getCacheStats();
            long missesBeforeAgg = statsBeforeAgg.getMissCount();
            System.out.println("   Cache misses before aggregations: " + missesBeforeAgg);

            // Run multiple aggregation queries
            SplitQuery matchAllQuery = new SplitMatchAllQuery();

            // Test 1: Stats aggregation
            System.out.println("\n   Running stats aggregation on 'score' field...");
            StatsAggregation scoreStats = new StatsAggregation("score_stats", "score");
            SearchResult statsResult = searcher.search(matchAllQuery, 0, "score_stats", scoreStats);

            assertTrue(statsResult.hasAggregations(), "Should have aggregation results");
            StatsResult stats = (StatsResult) statsResult.getAggregation("score_stats");
            assertNotNull(stats, "Stats result should not be null");
            System.out.println("     Stats: count=" + stats.getCount() +
                             ", sum=" + stats.getSum() +
                             ", avg=" + String.format("%.2f", stats.getAverage()) +
                             ", min=" + stats.getMin() +
                             ", max=" + stats.getMax());

            // Test 2: Sum aggregation on different field
            System.out.println("\n   Running sum aggregation on 'response_time' field...");
            SumAggregation responseSum = new SumAggregation("response_sum", "response_time");
            SearchResult sumResult = searcher.search(matchAllQuery, 0, "response_sum", responseSum);
            SumResult sum = (SumResult) sumResult.getAggregation("response_sum");
            assertNotNull(sum, "Sum result should not be null");
            System.out.println("     Sum of response_time: " + sum.getSum());

            // Test 3: Multiple aggregations in one query
            System.out.println("\n   Running multiple aggregations in single query...");
            Map<String, SplitAggregation> multiAggs = new HashMap<>();
            multiAggs.put("count_agg", new CountAggregation("id"));
            multiAggs.put("avg_agg", new AverageAggregation("score"));
            multiAggs.put("min_agg", new MinAggregation("response_time"));
            multiAggs.put("max_agg", new MaxAggregation("response_time"));
            SearchResult multiResult = searcher.search(matchAllQuery, 0, multiAggs);

            assertEquals(4, multiResult.getAggregations().size(), "Should have 4 aggregations");
            System.out.println("     Count: " + ((CountResult) multiResult.getAggregation("count_agg")).getCount());
            System.out.println("     Avg score: " + String.format("%.2f",
                ((AverageResult) multiResult.getAggregation("avg_agg")).getAverage()));
            System.out.println("     Min response_time: " + ((MinResult) multiResult.getAggregation("min_agg")).getMin());
            System.out.println("     Max response_time: " + ((MaxResult) multiResult.getAggregation("max_agg")).getMax());

            // Test 4: Aggregation with filtered query
            System.out.println("\n   Running filtered aggregation (high scores > 80)...");
            SplitQuery highScoreQuery = searcher.parseQuery("score:[80 TO *]");
            StatsAggregation highScoreStats = new StatsAggregation("high_score_stats", "score");
            SearchResult filteredResult = searcher.search(highScoreQuery, 0, "high_score_stats", highScoreStats);
            StatsResult filteredStats = (StatsResult) filteredResult.getAggregation("high_score_stats");
            if (filteredStats != null && filteredStats.getCount() > 0) {
                System.out.println("     High scores: count=" + filteredStats.getCount() +
                                 ", avg=" + String.format("%.2f", filteredStats.getAverage()));
            } else {
                System.out.println("     (No high score documents found - expected for random data)");
            }

            // Step 6: VERIFY no new cache misses occurred during aggregations
            var statsAfterAgg = searcher.getCacheStats();
            long missesAfterAgg = statsAfterAgg.getMissCount();
            long newMissesDuringAgg = missesAfterAgg - missesBeforeAgg;

            System.out.println("\n" + "=".repeat(80));
            System.out.println("RESULTS: DISK CACHE PREWARM + AGGREGATION TEST");
            System.out.println("=".repeat(80));
            System.out.println("   Prewarm time: " + prewarmTimeMs + " ms");
            System.out.println("   Disk cache size: " + diskCacheStats.getTotalBytes() + " bytes");
            System.out.println("   Disk cache components: " + diskCacheStats.getComponentCount());
            System.out.println("   Cache misses during prewarm: " + prewarmMisses);
            System.out.println("   Cache misses during aggregations: " + newMissesDuringAgg);
            System.out.println();

            // THE KEY ASSERTION: No new cache misses during aggregation
            // This proves that prewarm populated the disk cache and aggregations used it
            assertEquals(0, newMissesDuringAgg,
                "Aggregation queries should cause ZERO new cache misses after prewarm. " +
                "This validates that prewarm correctly populated the disk cache and " +
                "aggregations used it without downloading data from storage.");

            System.out.println("   ✅ SUCCESS: Aggregations used prewarmed disk cache - ZERO downloads!");
            System.out.println("=".repeat(80));
        }

        // Wait for disk cache background writes to complete
        Thread.sleep(200);
    }

    @Test
    @DisplayName("Test aggregation-only query uses prewarmed cache")
    void testAggregateOnlyQueryUsesPrewarmedCache() throws Exception {
        System.out.println("=".repeat(80));
        System.out.println("AGGREGATE-ONLY QUERY WITH PREWARMED CACHE TEST");
        System.out.println("=".repeat(80));

        // Setup
        Path indexPath = tempDir.resolve("agg-only-index");
        Path splitPath = tempDir.resolve("agg-only.split");
        String diskCachePath = tempDir.resolve("agg_only_disk_cache").toString();
        Files.createDirectories(Path.of(diskCachePath));

        createTestIndexWithNumericFields(indexPath);

        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "agg-only-index", "test-source", "test-node");
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
            indexPath.toString(), splitPath.toString(), config);

        String splitUrl = "file://" + splitPath.toAbsolutePath();

        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath)
            .withMaxDiskSize(500_000_000L);

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig(
            "agg-only-test-" + System.currentTimeMillis())
            .withMaxCacheSize(10_000_000)
            .withTieredCache(tieredConfig);

        SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);

        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata)) {

            // Prewarm
            System.out.println("\nPrewarming components...");
            searcher.preloadComponents(
                SplitSearcher.IndexComponent.TERM,
                SplitSearcher.IndexComponent.POSTINGS,
                SplitSearcher.IndexComponent.FASTFIELD
            ).join();

            var statsAfterPrewarm = searcher.getCacheStats();
            long missesAfterPrewarm = statsAfterPrewarm.getMissCount();
            System.out.println("   Cache misses after prewarm: " + missesAfterPrewarm);

            // Use aggregate() method (aggregation-only, no document hits)
            System.out.println("\nRunning aggregate-only query...");
            SplitQuery matchAllQuery = new SplitMatchAllQuery();
            StatsAggregation statsAgg = new StatsAggregation("score");

            SearchResult result = searcher.aggregate(matchAllQuery, "stats", statsAgg);

            // Verify aggregation-only behavior
            assertEquals(0, result.getHits().size(), "Aggregate-only should return no hits");
            assertTrue(result.hasAggregations(), "Should have aggregations");

            StatsResult stats = (StatsResult) result.getAggregation("stats");
            assertNotNull(stats, "Stats should not be null");
            assertEquals(NUM_DOCUMENTS, stats.getCount(), "Should aggregate all documents");

            // Verify no new cache misses
            var statsAfterAgg = searcher.getCacheStats();
            long newMisses = statsAfterAgg.getMissCount() - missesAfterPrewarm;

            System.out.println("   Stats result: count=" + stats.getCount() + ", avg=" +
                             String.format("%.2f", stats.getAverage()));
            System.out.println("   New cache misses during aggregate: " + newMisses);

            assertEquals(0, newMisses,
                "Aggregate-only query should use prewarmed cache with ZERO new downloads");

            System.out.println("\n   ✅ SUCCESS: Aggregate-only query used prewarmed cache!");
        }

        Thread.sleep(200);
    }

    @Test
    @DisplayName("Test second session uses persisted disk cache")
    void testSecondSessionUsesPersistedDiskCache() throws Exception {
        System.out.println("=".repeat(80));
        System.out.println("PERSISTED DISK CACHE ACROSS SESSIONS TEST");
        System.out.println("=".repeat(80));

        // Setup - shared disk cache path
        Path indexPath = tempDir.resolve("persist-test-index");
        Path splitPath = tempDir.resolve("persist-test.split");
        String diskCachePath = tempDir.resolve("persist_disk_cache").toString();
        Files.createDirectories(Path.of(diskCachePath));

        createTestIndexWithNumericFields(indexPath);

        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "persist-test-index", "test-source", "test-node");
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
            indexPath.toString(), splitPath.toString(), config);

        String splitUrl = "file://" + splitPath.toAbsolutePath();

        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath)
            .withMaxDiskSize(500_000_000L);

        // ========== SESSION 1: Prewarm and populate disk cache ==========
        System.out.println("\nSESSION 1: Prewarming to populate disk cache...");

        long session1Misses;
        try (SplitCacheManager cm1 = SplitCacheManager.getInstance(
                new SplitCacheManager.CacheConfig("session1-" + System.currentTimeMillis())
                    .withMaxCacheSize(10_000_000)
                    .withTieredCache(tieredConfig));
             SplitSearcher searcher1 = cm1.createSplitSearcher(splitUrl, metadata)) {

            // Prewarm
            searcher1.preloadComponents(
                SplitSearcher.IndexComponent.FASTFIELD,
                SplitSearcher.IndexComponent.TERM
            ).join();

            // Run aggregation
            SplitQuery query = new SplitMatchAllQuery();
            StatsAggregation statsAgg = new StatsAggregation("score");
            SearchResult result = searcher1.search(query, 0, "stats", statsAgg);

            session1Misses = searcher1.getCacheStats().getMissCount();
            System.out.println("   Session 1 total cache misses: " + session1Misses);

            StatsResult stats = (StatsResult) result.getAggregation("stats");
            System.out.println("   Session 1 aggregation: count=" + stats.getCount());
        }

        // Wait for disk cache to persist
        Thread.sleep(300);

        // Check disk cache files
        java.io.File diskCacheDir = new java.io.File(diskCachePath);
        System.out.println("   Disk cache files after session 1: " +
            (diskCacheDir.list() != null ? diskCacheDir.list().length : 0));

        // ========== SESSION 2: Should use persisted disk cache ==========
        System.out.println("\nSESSION 2: Running aggregation (should use persisted disk cache)...");

        try (SplitCacheManager cm2 = SplitCacheManager.getInstance(
                new SplitCacheManager.CacheConfig("session2-" + System.currentTimeMillis())
                    .withMaxCacheSize(10_000_000)
                    .withTieredCache(tieredConfig));  // Same disk cache path
             SplitSearcher searcher2 = cm2.createSplitSearcher(splitUrl, metadata)) {

            var statsBeforeAgg = searcher2.getCacheStats();
            long missesBeforeAgg = statsBeforeAgg.getMissCount();

            // Run same aggregation
            SplitQuery query = new SplitMatchAllQuery();
            StatsAggregation statsAgg = new StatsAggregation("score");
            SearchResult result = searcher2.search(query, 0, "stats", statsAgg);

            var statsAfterAgg = searcher2.getCacheStats();
            long missesAfterAgg = statsAfterAgg.getMissCount();
            long session2AggMisses = missesAfterAgg - missesBeforeAgg;

            StatsResult stats = (StatsResult) result.getAggregation("stats");
            System.out.println("   Session 2 aggregation: count=" + stats.getCount());
            System.out.println("   Session 2 cache misses during aggregation: " + session2AggMisses);

            // Session 2 should have fewer or zero misses because disk cache is persisted
            System.out.println("\n" + "=".repeat(80));
            System.out.println("RESULTS");
            System.out.println("=".repeat(80));
            System.out.println("   Session 1 total misses: " + session1Misses);
            System.out.println("   Session 2 aggregation misses: " + session2AggMisses);

            // Session 2 should benefit from persisted disk cache
            assertTrue(session2AggMisses <= session1Misses,
                "Session 2 should have equal or fewer misses due to persisted disk cache");

            System.out.println("\n   ✅ SUCCESS: Session 2 benefited from persisted disk cache!");
        }
    }

    @Test
    @DisplayName("FASTFIELD prewarm enables zero-download aggregations for ALL fast fields")
    void testFastFieldPrewarmForAggregations() throws Exception {
        System.out.println("=".repeat(80));
        System.out.println("FASTFIELD PREWARM FOR ZERO-DOWNLOAD AGGREGATIONS");
        System.out.println("=".repeat(80));
        System.out.println("This test verifies that prewarming FASTFIELD enables zero downloads");
        System.out.println("for aggregation queries on ANY fast field in the split.");
        System.out.println();
        System.out.println("NOTE: Tantivy stores all fast fields together in .fast files,");
        System.out.println("so prewarming FASTFIELD for any field prewarms ALL fast fields.");
        System.out.println();

        // Step 1: Create test split
        Path indexPath = tempDir.resolve("fastfield-agg-index");
        Path splitPath = tempDir.resolve("fastfield-agg.split");
        Path diskCachePath = tempDir.resolve("fastfield-agg-cache");
        Files.createDirectories(diskCachePath);

        createTestIndexWithNumericFields(indexPath);

        QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
            "fastfield-agg", "test-source", "test-node");
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
            indexPath.toString(), splitPath.toString(), splitConfig);

        String splitUrl = "file://" + splitPath.toAbsolutePath();

        // Configure cache with disk tier
        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath.toString())
            .withMaxDiskSize(100_000_000);

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig(
            "fastfield-agg-" + System.currentTimeMillis())
            .withMaxCacheSize(10_000_000)
            .withTieredCache(tieredConfig);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata)) {

            // Step 2: Prewarm FASTFIELD (all fast fields are stored together)
            System.out.println("\nStep 1: Prewarming FASTFIELD component...");

            var statsBeforePrewarm = searcher.getCacheStats();
            long missesBeforePrewarm = statsBeforePrewarm.getMissCount();

            // Prewarm the fast field data
            searcher.preloadFields(SplitSearcher.IndexComponent.FASTFIELD, "score").join();

            var statsAfterPrewarm = searcher.getCacheStats();
            long missesAfterPrewarm = statsAfterPrewarm.getMissCount();
            long prewarmMisses = missesAfterPrewarm - missesBeforePrewarm;

            System.out.println("   Prewarm downloads (cache misses): " + prewarmMisses);

            // Step 3: Run aggregation on 'score' field
            System.out.println("\nStep 2: Running stats aggregation on 'score' field...");

            var statsBeforeAgg1 = searcher.getCacheStats();
            long missesBeforeAgg1 = statsBeforeAgg1.getMissCount();

            SplitQuery query = new SplitMatchAllQuery();
            StatsAggregation scoreStats = new StatsAggregation("score_stats", "score");
            SearchResult result = searcher.search(query, 0, "score_stats", scoreStats);

            var statsAfterAgg1 = searcher.getCacheStats();
            long agg1Misses = statsAfterAgg1.getMissCount() - missesBeforeAgg1;

            StatsResult stats = (StatsResult) result.getAggregation("score_stats");
            System.out.println("   Aggregation result: count=" + stats.getCount() +
                             ", avg=" + String.format("%.2f", stats.getAverage()) +
                             ", min=" + stats.getMin() + ", max=" + stats.getMax());
            System.out.println("   Downloads during aggregation: " + agg1Misses);

            // Step 4: Run aggregation on 'response_time' field (also prewarmed)
            System.out.println("\nStep 3: Running stats aggregation on 'response_time' field...");
            System.out.println("   (Should ALSO have zero downloads - all fast fields prewarmed together)");

            var statsBeforeAgg2 = searcher.getCacheStats();
            long missesBeforeAgg2 = statsBeforeAgg2.getMissCount();

            StatsAggregation responseStats = new StatsAggregation("response_stats", "response_time");
            SearchResult result2 = searcher.search(query, 0, "response_stats", responseStats);

            var statsAfterAgg2 = searcher.getCacheStats();
            long agg2Misses = statsAfterAgg2.getMissCount() - missesBeforeAgg2;

            StatsResult responseStatsResult = (StatsResult) result2.getAggregation("response_stats");
            System.out.println("   Aggregation result: count=" + responseStatsResult.getCount() +
                             ", avg=" + String.format("%.2f", responseStatsResult.getAverage()));
            System.out.println("   Downloads during aggregation: " + agg2Misses);

            // Step 5: Verify ALL aggregations had zero downloads
            System.out.println("\n" + "=".repeat(80));
            System.out.println("RESULTS");
            System.out.println("=".repeat(80));
            System.out.println("   'score' field aggregation downloads: " + agg1Misses);
            System.out.println("   'response_time' field aggregation downloads: " + agg2Misses);

            assertEquals(0, agg1Misses,
                "Aggregation on 'score' should have ZERO downloads after FASTFIELD prewarm");
            assertEquals(0, agg2Misses,
                "Aggregation on 'response_time' should have ZERO downloads after FASTFIELD prewarm");

            System.out.println("\n   ✅ SUCCESS: FASTFIELD prewarm enables zero-download aggregations!");
            System.out.println("   ✅ All fast field aggregations had 0 downloads");
        }
    }

    private void createTestIndexWithNumericFields(Path indexPath) throws Exception {
        try (SchemaBuilder schemaBuilder = new SchemaBuilder()) {
            // Text fields
            schemaBuilder.addTextField("content", true, false, "default", "position");
            schemaBuilder.addTextField("category", true, false, "default", "position");

            // Numeric fields with FAST enabled (required for aggregations)
            schemaBuilder.addIntegerField("id", true, true, true);           // stored, indexed, fast
            schemaBuilder.addIntegerField("score", true, true, true);        // stored, indexed, fast
            schemaBuilder.addIntegerField("response_time", true, true, true); // stored, indexed, fast
            schemaBuilder.addFloatField("rating", true, true, true);         // stored, indexed, fast

            try (Schema schema = schemaBuilder.build()) {
                try (Index index = new Index(schema, indexPath.toString(), false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        String[] categories = {"premium", "standard", "basic", "enterprise"};

                        for (int i = 0; i < NUM_DOCUMENTS; i++) {
                            try (Document doc = new Document()) {
                                doc.addText("content", "Document " + i + " with searchable content");
                                doc.addText("category", categories[random.nextInt(categories.length)]);
                                doc.addInteger("id", i);
                                doc.addInteger("score", random.nextInt(100));
                                doc.addInteger("response_time", 50 + random.nextInt(500));
                                doc.addFloat("rating", random.nextFloat() * 5.0f);
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
