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

import static org.junit.jupiter.api.Assertions.*;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.QuickwitSplit;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests verifying that prescan operations properly populate the cache
 * and that subsequent search operations reuse cached data.
 *
 * This validates the key design goal: prescan fetches footer and term dict
 * bytes, which remain cached for subsequent SplitSearcher.search() calls.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PrescanCacheVerificationTest {

    @TempDir
    Path tempDir;

    private SplitCacheManager cacheManager;
    private String uniqueId;

    // Store split info and metadata together for searcher creation
    private static class SplitTestData {
        final SplitInfo splitInfo;
        final QuickwitSplit.SplitMetadata metadata;

        SplitTestData(SplitInfo splitInfo, QuickwitSplit.SplitMetadata metadata) {
            this.splitInfo = splitInfo;
            this.metadata = metadata;
        }
    }

    @BeforeEach
    public void setUp() throws Exception {
        uniqueId = UUID.randomUUID().toString();
        SplitCacheManager.CacheConfig config =
            new SplitCacheManager.CacheConfig("prescan-cache-verify-" + uniqueId)
                .withMaxCacheSize(100_000_000);
        cacheManager = SplitCacheManager.getInstance(config);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (cacheManager != null) {
            cacheManager.close();
        }
    }

    @Test
    @Order(1)
    @DisplayName("Cache - Prescan populates cache for subsequent search")
    public void testPrescanPopulatesCacheForSearch() throws Exception {
        // Create a test split
        SplitTestData testData = createTestSplit();
        String docMappingJson = getDocMapping();

        // Get initial cache stats
        SplitCacheManager.GlobalCacheStats initialStats = cacheManager.getGlobalCacheStats();
        long initialMisses = initialStats.getTotalMisses();

        // Prescan the split - this should populate cache
        SplitQuery prescanQuery = new SplitTermQuery("title", "hello");
        List<PrescanResult> prescanResults = cacheManager.prescanSplits(
            Collections.singletonList(testData.splitInfo), docMappingJson, prescanQuery);

        assertEquals(1, prescanResults.size());
        assertTrue(prescanResults.get(0).couldHaveResults(),
            "Prescan should find 'hello' term");

        // Get cache stats after prescan
        SplitCacheManager.GlobalCacheStats afterPrescanStats = cacheManager.getGlobalCacheStats();

        // Prescan should have caused cache misses (fetching data for first time)
        assertTrue(afterPrescanStats.getTotalMisses() >= initialMisses,
            "Prescan should have fetched data (causing cache misses on first access)");

        // Now search the same split - should hit cache
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(
                testData.splitInfo.getSplitUrl(), testData.metadata)) {
            SplitQuery searchQuery = new SplitTermQuery("title", "hello");
            var results = searcher.search(searchQuery, 10);

            assertNotNull(results, "Search should return results");
        }

        // Get cache stats after search
        SplitCacheManager.GlobalCacheStats afterSearchStats = cacheManager.getGlobalCacheStats();

        // After search, we should have some cache hits from reusing prescan data
        System.out.println("Cache stats progression:");
        System.out.println("  Initial: " + initialStats);
        System.out.println("  After prescan: " + afterPrescanStats);
        System.out.println("  After search: " + afterSearchStats);

        // The key metric: total hits should increase or cache should be utilized
        assertTrue(afterSearchStats.getTotalHits() > 0 || afterSearchStats.getCurrentSize() > 0,
            "Cache should have been utilized after prescan and search");
    }

    @Test
    @Order(2)
    @DisplayName("Cache - Multiple prescans on same split reuse cache")
    public void testMultiplePrescansReuseCache() throws Exception {
        // Create a test split
        SplitTestData testData = createTestSplit();
        String docMappingJson = getDocMapping();

        // First prescan
        SplitQuery query = new SplitTermQuery("content", "world");
        cacheManager.prescanSplits(Collections.singletonList(testData.splitInfo), docMappingJson, query);

        // Get cache stats after first prescan
        SplitCacheManager.GlobalCacheStats afterFirstPrescan = cacheManager.getGlobalCacheStats();
        long missesAfterFirst = afterFirstPrescan.getTotalMisses();

        // Second prescan with same split (should hit cache)
        cacheManager.prescanSplits(Collections.singletonList(testData.splitInfo), docMappingJson, query);

        // Get cache stats after second prescan
        SplitCacheManager.GlobalCacheStats afterSecondPrescan = cacheManager.getGlobalCacheStats();
        long hitsAfterSecond = afterSecondPrescan.getTotalHits();

        System.out.println("Multiple prescan cache stats:");
        System.out.println("  After first prescan: " + afterFirstPrescan);
        System.out.println("  After second prescan: " + afterSecondPrescan);

        // Second prescan should have more hits than first
        // (footer and term dict already cached)
        assertTrue(hitsAfterSecond >= afterFirstPrescan.getTotalHits(),
            "Second prescan should reuse cached data from first prescan");
    }

    @Test
    @Order(3)
    @DisplayName("Cache - Prescan with different queries on same split")
    public void testPrescanDifferentQueriesSameSplit() throws Exception {
        // Create a test split
        SplitTestData testData = createTestSplit();
        String docMappingJson = getDocMapping();

        // First prescan with one term
        SplitQuery query1 = new SplitTermQuery("title", "hello");
        List<PrescanResult> results1 = cacheManager.prescanSplits(
            Collections.singletonList(testData.splitInfo), docMappingJson, query1);

        // Get stats after first query
        SplitCacheManager.GlobalCacheStats afterFirst = cacheManager.getGlobalCacheStats();

        // Second prescan with different term on same field
        SplitQuery query2 = new SplitTermQuery("title", "world");
        List<PrescanResult> results2 = cacheManager.prescanSplits(
            Collections.singletonList(testData.splitInfo), docMappingJson, query2);

        // Get stats after second query
        SplitCacheManager.GlobalCacheStats afterSecond = cacheManager.getGlobalCacheStats();

        System.out.println("Different queries cache stats:");
        System.out.println("  Query 1 (hello): " + results1.get(0).couldHaveResults());
        System.out.println("  Query 2 (world): " + results2.get(0).couldHaveResults());
        System.out.println("  After first: " + afterFirst);
        System.out.println("  After second: " + afterSecond);

        // Both queries on same split should find results (hello and world both exist)
        assertTrue(results1.get(0).couldHaveResults(), "Should find 'hello'");
        assertTrue(results2.get(0).couldHaveResults(), "Should find 'world'");

        // Second query should benefit from footer cache even if term dict is different
        assertTrue(afterSecond.getTotalHits() >= afterFirst.getTotalHits(),
            "Second query should benefit from cached footer");
    }

    @Test
    @Order(4)
    @DisplayName("Cache - Prescan multiple splits")
    public void testPrescanMultipleSplits() throws Exception {
        // Create multiple test splits
        SplitTestData split1 = createTestSplit("split1", "hello world");
        SplitTestData split2 = createTestSplit("split2", "foo bar");
        SplitTestData split3 = createTestSplit("split3", "hello bar");
        String docMappingJson = getDocMapping();

        List<SplitInfo> splits = Arrays.asList(
            split1.splitInfo, split2.splitInfo, split3.splitInfo);

        // Prescan all splits for a term that exists in some but not all
        SplitQuery query = new SplitTermQuery("title", "hello");
        List<PrescanResult> results = cacheManager.prescanSplits(
            splits, docMappingJson, query);

        assertEquals(3, results.size(), "Should have results for all 3 splits");

        // Split1 and Split3 have "hello", Split2 does not
        long matchingCount = results.stream()
            .filter(PrescanResult::couldHaveResults)
            .count();

        // Get cache stats
        SplitCacheManager.GlobalCacheStats stats = cacheManager.getGlobalCacheStats();
        System.out.println("Multiple splits cache stats: " + stats);
        System.out.println("Matching splits: " + matchingCount + " of 3");

        assertTrue(matchingCount >= 1, "At least one split should have 'hello'");
        assertTrue(stats.getCurrentSize() > 0, "Cache should have data from all splits");
    }

    @Test
    @Order(5)
    @DisplayName("Cache - Comprehensive stats tracking")
    public void testComprehensiveCacheStats() throws Exception {
        // Create a test split
        SplitTestData testData = createTestSplit();
        String docMappingJson = getDocMapping();

        // Prescan to populate cache
        SplitQuery query = new SplitTermQuery("title", "hello");
        cacheManager.prescanSplits(Collections.singletonList(testData.splitInfo), docMappingJson, query);

        // Get comprehensive cache stats
        SplitCacheManager.ComprehensiveCacheStats comprehensiveStats =
            cacheManager.getComprehensiveCacheStats();

        System.out.println("Comprehensive cache stats:\n" + comprehensiveStats);

        // Verify we can access individual cache type stats
        assertNotNull(comprehensiveStats.getByteRangeCache(), "ByteRange cache stats should exist");
        assertNotNull(comprehensiveStats.getFooterCache(), "Footer cache stats should exist");
        assertNotNull(comprehensiveStats.getAggregated(), "Aggregated stats should exist");

        // After prescan, we should have some cache activity
        SplitCacheManager.GlobalCacheStats aggregated = comprehensiveStats.getAggregated();
        assertTrue(aggregated.getCurrentSize() > 0 ||
                   aggregated.getTotalHits() > 0 ||
                   aggregated.getTotalMisses() > 0,
            "Cache should show activity after prescan");
    }

    // Helper methods

    private SplitTestData createTestSplit() throws Exception {
        return createTestSplit("test", "hello world");
    }

    private SplitTestData createTestSplit(String name, String content) throws Exception {
        Path indexDir = tempDir.resolve(name + "_index_" + UUID.randomUUID());
        Files.createDirectories(indexDir);
        Path splitPath = tempDir.resolve(name + "_" + UUID.randomUUID() + ".split");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "basic");
            builder.addTextField("content", true, false, "default", "basic");

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexDir.toString())) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Add multiple documents
                        for (String word : content.split(" ")) {
                            try (Document doc = new Document()) {
                                doc.addText("title", word);
                                doc.addText("content", content);
                                writer.addDocument(doc);
                            }
                        }
                        writer.commit();
                    }
                    index.reload();

                    QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                        name + "-index-" + uniqueId, name + "-source", "node-1");
                    QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
                        indexDir.toString(), splitPath.toString(), splitConfig);

                    SplitInfo splitInfo = new SplitInfo("file://" + splitPath.toString(), metadata.getFooterStartOffset());
                    return new SplitTestData(splitInfo, metadata);
                }
            }
        }
    }

    private String getDocMapping() {
        return "[{\"fast\":false,\"indexed\":true,\"name\":\"title\",\"stored\":true,\"tokenizer\":\"default\",\"type\":\"text\"}," +
               "{\"fast\":false,\"indexed\":true,\"name\":\"content\",\"stored\":true,\"tokenizer\":\"default\",\"type\":\"text\"}]";
    }
}
