package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.Map;
import java.util.HashMap;
import java.nio.file.Path;

/**
 * Comprehensive test suite for SplitSearcher aggregation functionality.
 * Tests the implementation of count, sum, average, min, and max aggregations
 * following Quickwit's aggregation model.
 */
public class SplitSearcherAggregationTest {

    private SplitCacheManager cacheManager;
    private SplitSearcher searcher;
    private String splitPath;
    private QuickwitSplit.SplitMetadata metadata;

    @BeforeEach
    public void setUp(@TempDir Path tempDir) throws Exception {
        // Create test index with numeric data - use unique names to avoid cache conflicts
        String uniqueId = "aggregation_test_" + System.nanoTime();
        String indexPath = tempDir.resolve(uniqueId + "_index").toString();
        splitPath = tempDir.resolve(uniqueId + ".split").toString();

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder
                .addTextField("name", true, false, "default", "position")
                .addTextField("category", true, false, "default", "position")
                .addIntegerField("score", true, true, true)     // stored + indexed + fast
                .addIntegerField("response_time", true, true, true) // stored + indexed + fast
                .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        // Add test documents with varying numeric values
                        try (Document doc1 = new Document()) {
                            doc1.addInteger("id", 1);
                            doc1.addText("name", "Alice");
                            doc1.addText("category", "premium");
                            doc1.addInteger("score", 85);
                            doc1.addInteger("response_time", 120);
                            writer.addDocument(doc1);
                        }

                        try (Document doc2 = new Document()) {
                            doc2.addInteger("id", 2);
                            doc2.addText("name", "Bob");
                            doc2.addText("category", "standard");
                            doc2.addInteger("score", 75);
                            doc2.addInteger("response_time", 200);
                            writer.addDocument(doc2);
                        }

                        try (Document doc3 = new Document()) {
                            doc3.addInteger("id", 3);
                            doc3.addText("name", "Charlie");
                            doc3.addText("category", "premium");
                            doc3.addInteger("score", 95);
                            doc3.addInteger("response_time", 80);
                            writer.addDocument(doc3);
                        }

                        try (Document doc4 = new Document()) {
                            doc4.addInteger("id", 4);
                            doc4.addText("name", "Diana");
                            doc4.addText("category", "basic");
                            doc4.addInteger("score", 60);
                            doc4.addInteger("response_time", 300);
                            writer.addDocument(doc4);
                        }

                        try (Document doc5 = new Document()) {
                            doc5.addInteger("id", 5);
                            doc5.addText("name", "Eve");
                            doc5.addText("category", "premium");
                            doc5.addInteger("score", 90);
                            doc5.addInteger("response_time", 100);
                            writer.addDocument(doc5);
                        }

                        writer.commit();
                    }

                    // Convert to QuickwitSplit - use unique ID to avoid cache conflicts
                    QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                        uniqueId, "test-source", "test-node");

                    // Get the metadata with footer offsets from the conversion
                    QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);

                    // Store metadata for searcher creation
                    this.metadata = metadata;
                }
            }
        }

        // Create cache manager and searcher with unique name per test run
        String uniqueCacheName = uniqueId + "-cache";
        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig(uniqueCacheName);
        cacheManager = SplitCacheManager.getInstance(cacheConfig);

        // Use the metadata with proper footer offsets from convertIndexFromPath
        searcher = cacheManager.createSplitSearcher("file://" + splitPath, metadata);
    }

    @AfterEach
    public void tearDown() {
        if (searcher != null) {
            searcher.close();
        }
    }

    @Test
    @DisplayName("Test basic stats aggregation (count, sum, avg, min, max)")
    public void testStatsAggregation() {
        System.out.println("🧮 Testing stats aggregation...");

        // Create stats aggregation for score field
        StatsAggregation scoreStats = new StatsAggregation("score_stats", "score");
        SplitQuery matchAllQuery = new SplitMatchAllQuery();

        // Perform aggregation
        SearchResult result = searcher.search(matchAllQuery, 10, "stats", scoreStats);

        // Verify aggregation result
        assertTrue(result.hasAggregations(), "Result should contain aggregations");
        StatsResult stats = (StatsResult) result.getAggregation("stats");
        assertNotNull(stats, "Stats aggregation result should not be null");

        // Verify stats values
        // Data: 85, 75, 95, 60, 90
        assertEquals(5, stats.getCount(), "Count should be 5");
        assertEquals(405.0, stats.getSum(), 0.01, "Sum should be 405");
        assertEquals(81.0, stats.getAverage(), 0.01, "Average should be 81");
        assertEquals(60.0, stats.getMin(), 0.01, "Min should be 60");
        assertEquals(95.0, stats.getMax(), 0.01, "Max should be 95");

        System.out.println("✅ Stats aggregation: " + stats);
    }

    @Test
    @DisplayName("Test individual aggregations (count, sum, avg, min, max)")
    public void testIndividualAggregations() {
        System.out.println("🔢 Testing individual aggregations...");

        SplitQuery matchAllQuery = new SplitMatchAllQuery();

        // Test Count Aggregation - using "id" field which exists in our test data
        CountAggregation countAgg = new CountAggregation("doc_count", "id");
        SearchResult countResult = searcher.search(matchAllQuery, 10, "count", countAgg);
        CountResult count = (CountResult) countResult.getAggregation("count");
        assertNotNull(count, "Count result should not be null");
        assertEquals(5, count.getCount(), "Document count should be 5");
        System.out.println("✅ Count: " + count);

        // Test Sum Aggregation
        SumAggregation sumAgg = new SumAggregation("score_sum", "score");
        SearchResult sumResult = searcher.search(matchAllQuery, 10, "sum", sumAgg);
        SumResult sum = (SumResult) sumResult.getAggregation("sum");
        assertNotNull(sum, "Sum result should not be null");
        assertEquals(405.0, sum.getSum(), 0.01, "Sum should be 405");
        System.out.println("✅ Sum: " + sum);

        // Test Average Aggregation
        AverageAggregation avgAgg = new AverageAggregation("score_avg", "score");
        SearchResult avgResult = searcher.search(matchAllQuery, 10, "avg", avgAgg);
        AverageResult avg = (AverageResult) avgResult.getAggregation("avg");
        assertNotNull(avg, "Average result should not be null");
        assertEquals(81.0, avg.getAverage(), 0.01, "Average should be 81");
        System.out.println("✅ Average: " + avg);

        // Test Min Aggregation
        MinAggregation minAgg = new MinAggregation("score_min", "score");
        SearchResult minResult = searcher.search(matchAllQuery, 10, "min", minAgg);
        MinResult min = (MinResult) minResult.getAggregation("min");
        assertNotNull(min, "Min result should not be null");
        assertEquals(60.0, min.getMin(), 0.01, "Min should be 60");
        System.out.println("✅ Min: " + min);

        // Test Max Aggregation
        MaxAggregation maxAgg = new MaxAggregation("score_max", "score");
        SearchResult maxResult = searcher.search(matchAllQuery, 10, "max", maxAgg);
        MaxResult max = (MaxResult) maxResult.getAggregation("max");
        assertNotNull(max, "Max result should not be null");
        assertEquals(95.0, max.getMax(), 0.01, "Max should be 95");
        System.out.println("✅ Max: " + max);
    }

    @Test
    @DisplayName("Test multiple aggregations in single search")
    public void testMultipleAggregations() {
        System.out.println("📊 Testing multiple aggregations...");

        SplitQuery matchAllQuery = new SplitMatchAllQuery();

        // Create multiple aggregations
        Map<String, SplitAggregation> aggregations = new HashMap<>();
        aggregations.put("score_stats", new StatsAggregation("score"));
        aggregations.put("response_sum", new SumAggregation("response_time"));
        aggregations.put("doc_count", new CountAggregation("id"));

        // Perform search with multiple aggregations
        SearchResult result = searcher.search(matchAllQuery, 10, aggregations);

        // Verify all aggregations are present
        assertTrue(result.hasAggregations(), "Result should contain aggregations");
        assertEquals(3, result.getAggregations().size(), "Should have 3 aggregations");

        // Verify score stats
        StatsResult scoreStats = (StatsResult) result.getAggregation("score_stats");
        assertNotNull(scoreStats, "Score stats should not be null");
        assertEquals(5, scoreStats.getCount(), "Score count should be 5");
        assertEquals(81.0, scoreStats.getAverage(), 0.01, "Score average should be 81");

        // Verify response time sum
        SumResult responseSum = (SumResult) result.getAggregation("response_sum");
        assertNotNull(responseSum, "Response sum should not be null");
        assertEquals(800.0, responseSum.getSum(), 0.01, "Response sum should be 800 (120+200+80+300+100)");

        // Verify document count
        CountResult docCount = (CountResult) result.getAggregation("doc_count");
        assertNotNull(docCount, "Doc count should not be null");
        assertEquals(5, docCount.getCount(), "Document count should be 5");

        System.out.println("✅ Multiple aggregations completed successfully");
        System.out.println("   Score stats: " + scoreStats);
        System.out.println("   Response sum: " + responseSum);
        System.out.println("   Doc count: " + docCount);
    }

    @Test
    @DisplayName("Test aggregation-only search (no document hits)")
    public void testAggregationOnlySearch() {
        System.out.println("🎯 Testing aggregation-only search...");

        SplitQuery matchAllQuery = new SplitMatchAllQuery();
        StatsAggregation scoreStats = new StatsAggregation("score");

        // Perform aggregation-only search
        SearchResult result = searcher.aggregate(matchAllQuery, "stats", scoreStats);

        // Verify no hits but aggregations present
        assertEquals(0, result.getHits().size(), "Should have no document hits");
        assertTrue(result.hasAggregations(), "Should have aggregations");

        StatsResult stats = (StatsResult) result.getAggregation("stats");
        assertNotNull(stats, "Stats should not be null");
        assertEquals(5, stats.getCount(), "Count should still be 5");
        assertEquals(81.0, stats.getAverage(), 0.01, "Average should still be 81");

        System.out.println("✅ Aggregation-only search: " + stats);
    }

    @Test
    @DisplayName("Test aggregations with filtered query")
    public void testAggregationsWithFilter() {
        System.out.println("🔍 Testing aggregations with filtered query...");

        // Create query for premium category only
        SplitQuery premiumQuery = searcher.parseQuery("premium", "category");
        StatsAggregation scoreStats = new StatsAggregation("score");

        // Perform filtered aggregation
        SearchResult result = searcher.search(premiumQuery, 10, "stats", scoreStats);

        // Verify aggregation on filtered data
        StatsResult stats = (StatsResult) result.getAggregation("stats");
        assertNotNull(stats, "Stats should not be null");

        // Premium users: Alice (85), Charlie (95), Eve (90) = 3 docs, sum=270, avg=90
        assertEquals(3, stats.getCount(), "Should have 3 premium users");
        assertEquals(270.0, stats.getSum(), 0.01, "Premium sum should be 270");
        assertEquals(90.0, stats.getAverage(), 0.01, "Premium average should be 90");
        assertEquals(85.0, stats.getMin(), 0.01, "Premium min should be 85");
        assertEquals(95.0, stats.getMax(), 0.01, "Premium max should be 95");

        System.out.println("✅ Filtered aggregation (premium users): " + stats);
    }

    @Test
    @DisplayName("Test aggregation JSON generation")
    public void testAggregationJsonGeneration() {
        System.out.println("🔧 Testing aggregation JSON generation...");

        // Test various aggregation JSON outputs
        StatsAggregation statsAgg = new StatsAggregation("test_stats", "score");
        assertEquals("{\"stats\": {\"field\": \"score\"}}", statsAgg.toAggregationJson());

        SumAggregation sumAgg = new SumAggregation("test_sum", "response_time");
        assertEquals("{\"sum\": {\"field\": \"response_time\"}}", sumAgg.toAggregationJson());

        AverageAggregation avgAgg = new AverageAggregation("test_avg", "score");
        assertEquals("{\"avg\": {\"field\": \"score\"}}", avgAgg.toAggregationJson());

        MinAggregation minAgg = new MinAggregation("test_min", "score");
        assertEquals("{\"min\": {\"field\": \"score\"}}", minAgg.toAggregationJson());

        MaxAggregation maxAgg = new MaxAggregation("test_max", "score");
        assertEquals("{\"max\": {\"field\": \"score\"}}", maxAgg.toAggregationJson());

        CountAggregation countAgg = new CountAggregation("test_count", "id");
        assertEquals("{\"value_count\": {\"field\": \"id\"}}", countAgg.toAggregationJson());

        System.out.println("✅ All aggregation JSON formats validated");
    }

    @Test
    @DisplayName("Test aggregation error handling")
    public void testAggregationErrorHandling() {
        System.out.println("⚠️ Testing aggregation error handling...");

        // Test null field name
        assertThrows(IllegalArgumentException.class, () -> {
            new StatsAggregation(null);
        }, "Should throw exception for null field name");

        assertThrows(IllegalArgumentException.class, () -> {
            new SumAggregation("");
        }, "Should throw exception for empty field name");

        // Test null aggregation name
        assertThrows(IllegalArgumentException.class, () -> {
            new AverageAggregation(null, "score");
        }, "Should throw exception for null aggregation name");

        System.out.println("✅ Error handling validated");
    }
}