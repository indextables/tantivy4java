package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;
import io.indextables.tantivy4java.aggregation.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.Map;
import java.util.HashMap;
import java.time.LocalDateTime;
import java.time.Instant;
import java.time.ZoneId;

/**
 * Comprehensive test suite for HistogramAggregation functionality.
 * Tests all histogram features including:
 * - Various interval sizes
 * - Offset parameter
 * - Extended bounds
 * - Hard bounds
 * - Min doc count filtering
 * - Keyed output format
 * - Sub-aggregations
 * - Edge cases
 */
public class HistogramAggregationTest {

    private SplitCacheManager cacheManager;
    private SplitSearcher searcher;
    private String splitPath;
    private QuickwitSplit.SplitMetadata metadata;
    private String testName;

    @BeforeEach
    public void setUp(@TempDir Path tempDir, TestInfo testInfo) throws Exception {
        testName = testInfo.getTestMethod().get().getName();

        String indexPath = tempDir.resolve(testName + "_index").toString();
        splitPath = tempDir.resolve(testName + ".split").toString();

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder
                .addTextField("category", true, true, "default", "position")
                .addTextField("name", true, true, "default", "position")
                .addIntegerField("price", true, true, true)
                .addIntegerField("quantity", true, true, true)
                .addFloatField("score", true, true, true)
                .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Create test data with specific distribution for histogram testing
                        // Prices: 10, 25, 50, 75, 100, 150, 200, 250, 500, 1000
                        addProduct(writer, 1, "electronics", "phone", 10, 5, 1.5f);
                        addProduct(writer, 2, "electronics", "tablet", 25, 3, 2.5f);
                        addProduct(writer, 3, "electronics", "laptop", 50, 2, 3.5f);
                        addProduct(writer, 4, "books", "novel", 75, 10, 4.0f);
                        addProduct(writer, 5, "books", "textbook", 100, 8, 4.5f);
                        addProduct(writer, 6, "clothing", "shirt", 150, 20, 3.0f);
                        addProduct(writer, 7, "clothing", "pants", 200, 15, 3.5f);
                        addProduct(writer, 8, "clothing", "jacket", 250, 5, 4.0f);
                        addProduct(writer, 9, "electronics", "tv", 500, 2, 4.5f);
                        addProduct(writer, 10, "electronics", "camera", 1000, 1, 5.0f);

                        writer.commit();
                    }

                    QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                        "histogram-test-" + testName, "test-source", "test-node");

                    metadata = QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);
                }
            }
        }

        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig("histogram-test-cache-" + testName);
        cacheManager = SplitCacheManager.getInstance(cacheConfig);
        searcher = cacheManager.createSplitSearcher("file://" + splitPath, metadata);
    }

    @AfterEach
    public void tearDown() {
        if (searcher != null) {
            searcher.close();
        }
        if (cacheManager != null) {
            cacheManager.close();
        }
    }

    private void addProduct(IndexWriter writer, int id, String category, String name,
                           int price, int quantity, float score) throws Exception {
        try (Document doc = new Document()) {
            doc.addInteger("id", id);
            doc.addText("category", category);
            doc.addText("name", name);
            doc.addInteger("price", price);
            doc.addInteger("quantity", quantity);
            doc.addFloat("score", score);
            writer.addDocument(doc);
        }
    }

    // ==================== Basic Histogram Tests ====================

    @Test
    @DisplayName("Basic histogram with interval=100")
    public void testBasicHistogram() {
        System.out.println("Testing basic histogram with interval=100...");

        SplitQuery query = new SplitMatchAllQuery();
        HistogramAggregation hist = new HistogramAggregation("price_hist", "price", 100.0);

        SearchResult result = searcher.search(query, 10, "price_hist", hist);

        assertTrue(result.hasAggregations(), "Should have aggregations");
        HistogramResult histResult = (HistogramResult) result.getAggregation("price_hist");
        assertNotNull(histResult, "Histogram result should not be null");

        // Verify buckets
        Map<Double, Long> buckets = new HashMap<>();
        for (HistogramResult.HistogramBucket bucket : histResult.getBuckets()) {
            buckets.put(bucket.getKey(), bucket.getDocCount());
            System.out.println("  Bucket " + bucket.getKey() + ": " + bucket.getDocCount() + " docs");
        }

        // 0-100: prices 10, 25, 50, 75 = 4 docs
        assertEquals(4L, buckets.get(0.0), "0-100 bucket should have 4 docs");
        // 100-200: prices 100, 150 = 2 docs
        assertEquals(2L, buckets.get(100.0), "100-200 bucket should have 2 docs");
        // 200-300: prices 200, 250 = 2 docs
        assertEquals(2L, buckets.get(200.0), "200-300 bucket should have 2 docs");
        // 500-600: price 500 = 1 doc
        assertEquals(1L, buckets.get(500.0), "500-600 bucket should have 1 doc");
        // 1000-1100: price 1000 = 1 doc
        assertEquals(1L, buckets.get(1000.0), "1000-1100 bucket should have 1 doc");

        System.out.println("Basic histogram test passed!");
    }

    @Test
    @DisplayName("Histogram with small interval=10")
    public void testSmallIntervalHistogram() {
        System.out.println("Testing histogram with small interval=10...");

        SplitQuery query = new SplitMatchAllQuery();
        HistogramAggregation hist = new HistogramAggregation("price_hist", "price", 10.0);

        SearchResult result = searcher.search(query, 10, "price_hist", hist);

        HistogramResult histResult = (HistogramResult) result.getAggregation("price_hist");
        assertNotNull(histResult, "Histogram result should not be null");

        // With interval=10, we should have more granular buckets
        assertTrue(histResult.getBuckets().size() >= 5, "Should have at least 5 buckets with interval=10");

        for (HistogramResult.HistogramBucket bucket : histResult.getBuckets()) {
            System.out.println("  Bucket " + bucket.getKey() + ": " + bucket.getDocCount() + " docs");
        }

        System.out.println("Small interval histogram test passed!");
    }

    @Test
    @DisplayName("Histogram with large interval=500")
    public void testLargeIntervalHistogram() {
        System.out.println("Testing histogram with large interval=500...");

        SplitQuery query = new SplitMatchAllQuery();
        HistogramAggregation hist = new HistogramAggregation("price_hist", "price", 500.0);

        SearchResult result = searcher.search(query, 10, "price_hist", hist);

        HistogramResult histResult = (HistogramResult) result.getAggregation("price_hist");
        assertNotNull(histResult, "Histogram result should not be null");

        Map<Double, Long> buckets = new HashMap<>();
        for (HistogramResult.HistogramBucket bucket : histResult.getBuckets()) {
            buckets.put(bucket.getKey(), bucket.getDocCount());
            System.out.println("  Bucket " + bucket.getKey() + ": " + bucket.getDocCount() + " docs");
        }

        // 0-500: prices 10, 25, 50, 75, 100, 150, 200, 250 = 8 docs
        assertEquals(8L, buckets.get(0.0), "0-500 bucket should have 8 docs");
        // 500-1000: price 500 = 1 doc
        assertEquals(1L, buckets.get(500.0), "500-1000 bucket should have 1 doc");
        // 1000-1500: price 1000 = 1 doc
        assertEquals(1L, buckets.get(1000.0), "1000-1500 bucket should have 1 doc");

        System.out.println("Large interval histogram test passed!");
    }

    // ==================== Offset Tests ====================

    @Test
    @DisplayName("Histogram with offset shifts bucket boundaries")
    public void testHistogramWithOffset() {
        System.out.println("Testing histogram with offset=50...");

        SplitQuery query = new SplitMatchAllQuery();
        HistogramAggregation hist = new HistogramAggregation("price_hist", "price", 100.0)
            .setOffset(50.0);

        SearchResult result = searcher.search(query, 10, "price_hist", hist);

        HistogramResult histResult = (HistogramResult) result.getAggregation("price_hist");
        assertNotNull(histResult, "Histogram result should not be null");

        // With offset=50, buckets are: -50 to 50, 50 to 150, 150 to 250, etc.
        for (HistogramResult.HistogramBucket bucket : histResult.getBuckets()) {
            System.out.println("  Bucket " + bucket.getKey() + ": " + bucket.getDocCount() + " docs");
            // Bucket keys should be offset by 50
            assertTrue(bucket.getKey() % 100 == 50 || bucket.getKey() == -50.0 || bucket.getKey() % 100 == -50,
                "Bucket key should be offset by 50");
        }

        System.out.println("Histogram with offset test passed!");
    }

    // ==================== Min Doc Count Tests ====================

    @Test
    @DisplayName("Histogram with min_doc_count filters empty buckets")
    public void testHistogramWithMinDocCount() {
        System.out.println("Testing histogram with min_doc_count=1...");

        SplitQuery query = new SplitMatchAllQuery();
        HistogramAggregation hist = new HistogramAggregation("price_hist", "price", 100.0)
            .setMinDocCount(1);

        SearchResult result = searcher.search(query, 10, "price_hist", hist);

        HistogramResult histResult = (HistogramResult) result.getAggregation("price_hist");
        assertNotNull(histResult, "Histogram result should not be null");

        // All returned buckets should have at least 1 document
        for (HistogramResult.HistogramBucket bucket : histResult.getBuckets()) {
            assertTrue(bucket.getDocCount() >= 1, "Bucket should have at least 1 doc with min_doc_count=1");
            System.out.println("  Bucket " + bucket.getKey() + ": " + bucket.getDocCount() + " docs");
        }

        // Should NOT have buckets for gaps like 300-400, 400-500, 600-700, etc.
        Map<Double, Long> buckets = new HashMap<>();
        for (HistogramResult.HistogramBucket bucket : histResult.getBuckets()) {
            buckets.put(bucket.getKey(), bucket.getDocCount());
        }
        assertFalse(buckets.containsKey(300.0), "300-400 bucket should be filtered out");
        assertFalse(buckets.containsKey(400.0), "400-500 bucket should be filtered out");

        System.out.println("Histogram with min_doc_count test passed!");
    }

    @Test
    @DisplayName("Histogram with min_doc_count=2 filters single-doc buckets")
    public void testHistogramWithMinDocCount2() {
        System.out.println("Testing histogram with min_doc_count=2...");

        SplitQuery query = new SplitMatchAllQuery();
        HistogramAggregation hist = new HistogramAggregation("price_hist", "price", 100.0)
            .setMinDocCount(2);

        SearchResult result = searcher.search(query, 10, "price_hist", hist);

        HistogramResult histResult = (HistogramResult) result.getAggregation("price_hist");
        assertNotNull(histResult, "Histogram result should not be null");

        // All returned buckets should have at least 2 documents
        for (HistogramResult.HistogramBucket bucket : histResult.getBuckets()) {
            assertTrue(bucket.getDocCount() >= 2, "Bucket should have at least 2 docs with min_doc_count=2");
            System.out.println("  Bucket " + bucket.getKey() + ": " + bucket.getDocCount() + " docs");
        }

        System.out.println("Histogram with min_doc_count=2 test passed!");
    }

    // ==================== Hard Bounds Tests ====================

    @Test
    @DisplayName("Histogram with hard_bounds filters values outside range")
    public void testHistogramWithHardBounds() {
        System.out.println("Testing histogram with hard_bounds 0 to 300...");

        SplitQuery query = new SplitMatchAllQuery();
        HistogramAggregation hist = new HistogramAggregation("price_hist", "price", 100.0)
            .setHardBounds(0.0, 300.0);

        SearchResult result = searcher.search(query, 10, "price_hist", hist);

        HistogramResult histResult = (HistogramResult) result.getAggregation("price_hist");
        assertNotNull(histResult, "Histogram result should not be null");

        // Should only have buckets within 0-300 range
        long totalDocs = 0;
        for (HistogramResult.HistogramBucket bucket : histResult.getBuckets()) {
            assertTrue(bucket.getKey() >= 0 && bucket.getKey() < 300,
                "Bucket key should be within hard bounds");
            totalDocs += bucket.getDocCount();
            System.out.println("  Bucket " + bucket.getKey() + ": " + bucket.getDocCount() + " docs");
        }

        // Prices 10, 25, 50, 75, 100, 150, 200, 250 = 8 docs within bounds
        // Prices 500, 1000 should be excluded
        assertEquals(8L, totalDocs, "Should have 8 docs within hard bounds 0-300");

        System.out.println("Histogram with hard_bounds test passed!");
    }

    // ==================== Extended Bounds Tests ====================

    @Test
    @DisplayName("Histogram with extended_bounds creates empty buckets")
    public void testHistogramWithExtendedBounds() {
        System.out.println("Testing histogram with extended_bounds 0 to 1500...");

        SplitQuery query = new SplitMatchAllQuery();
        HistogramAggregation hist = new HistogramAggregation("price_hist", "price", 100.0)
            .setExtendedBounds(0.0, 1500.0);

        SearchResult result = searcher.search(query, 10, "price_hist", hist);

        HistogramResult histResult = (HistogramResult) result.getAggregation("price_hist");
        assertNotNull(histResult, "Histogram result should not be null");

        // Should have buckets from 0 to 1500, including empty ones
        Map<Double, Long> buckets = new HashMap<>();
        for (HistogramResult.HistogramBucket bucket : histResult.getBuckets()) {
            buckets.put(bucket.getKey(), bucket.getDocCount());
            System.out.println("  Bucket " + bucket.getKey() + ": " + bucket.getDocCount() + " docs");
        }

        // Should have bucket for 1100, 1200, 1300, 1400 even if empty
        assertTrue(histResult.getBuckets().size() >= 10, "Should have extended range of buckets");

        System.out.println("Histogram with extended_bounds test passed!");
    }

    // ==================== Different Field Types ====================

    @Test
    @DisplayName("Histogram on quantity field")
    public void testHistogramOnQuantityField() {
        System.out.println("Testing histogram on quantity field...");

        SplitQuery query = new SplitMatchAllQuery();
        HistogramAggregation hist = new HistogramAggregation("qty_hist", "quantity", 5.0);

        SearchResult result = searcher.search(query, 10, "qty_hist", hist);

        HistogramResult histResult = (HistogramResult) result.getAggregation("qty_hist");
        assertNotNull(histResult, "Histogram result should not be null");

        long totalDocs = 0;
        for (HistogramResult.HistogramBucket bucket : histResult.getBuckets()) {
            totalDocs += bucket.getDocCount();
            System.out.println("  Bucket " + bucket.getKey() + ": " + bucket.getDocCount() + " docs");
        }

        assertEquals(10L, totalDocs, "Total docs should be 10");
        System.out.println("Histogram on quantity field test passed!");
    }

    @Test
    @DisplayName("Histogram on float score field")
    public void testHistogramOnFloatField() {
        System.out.println("Testing histogram on float score field...");

        SplitQuery query = new SplitMatchAllQuery();
        HistogramAggregation hist = new HistogramAggregation("score_hist", "score", 1.0);

        SearchResult result = searcher.search(query, 10, "score_hist", hist);

        HistogramResult histResult = (HistogramResult) result.getAggregation("score_hist");
        assertNotNull(histResult, "Histogram result should not be null");

        long totalDocs = 0;
        for (HistogramResult.HistogramBucket bucket : histResult.getBuckets()) {
            totalDocs += bucket.getDocCount();
            System.out.println("  Bucket " + bucket.getKey() + ": " + bucket.getDocCount() + " docs");
        }

        assertEquals(10L, totalDocs, "Total docs should be 10");
        System.out.println("Histogram on float field test passed!");
    }

    // ==================== Combined with Query Filter ====================

    @Test
    @DisplayName("Histogram filtered by category query")
    public void testHistogramWithQueryFilter() {
        System.out.println("Testing histogram filtered by category=electronics...");

        SplitQuery query = new SplitTermQuery("category", "electronics");
        HistogramAggregation hist = new HistogramAggregation("price_hist", "price", 100.0);

        SearchResult result = searcher.search(query, 10, "price_hist", hist);

        HistogramResult histResult = (HistogramResult) result.getAggregation("price_hist");
        assertNotNull(histResult, "Histogram result should not be null");

        // Electronics: phone(10), tablet(25), laptop(50), tv(500), camera(1000) = 5 docs
        long totalDocs = 0;
        for (HistogramResult.HistogramBucket bucket : histResult.getBuckets()) {
            totalDocs += bucket.getDocCount();
            System.out.println("  Bucket " + bucket.getKey() + ": " + bucket.getDocCount() + " docs");
        }

        assertEquals(5L, totalDocs, "Should have 5 electronics docs");
        System.out.println("Histogram with query filter test passed!");
    }

    // ==================== Multiple Histograms ====================

    @Test
    @DisplayName("Multiple histogram aggregations in one query")
    public void testMultipleHistograms() {
        System.out.println("Testing multiple histogram aggregations...");

        SplitQuery query = new SplitMatchAllQuery();

        Map<String, SplitAggregation> aggs = new HashMap<>();
        aggs.put("price_hist", new HistogramAggregation("price_hist", "price", 100.0));
        aggs.put("qty_hist", new HistogramAggregation("qty_hist", "quantity", 5.0));

        SearchResult result = searcher.search(query, 10, aggs);

        assertTrue(result.hasAggregations(), "Should have aggregations");
        assertEquals(2, result.getAggregations().size(), "Should have 2 aggregations");

        HistogramResult priceHist = (HistogramResult) result.getAggregation("price_hist");
        HistogramResult qtyHist = (HistogramResult) result.getAggregation("qty_hist");

        assertNotNull(priceHist, "Price histogram should not be null");
        assertNotNull(qtyHist, "Quantity histogram should not be null");

        System.out.println("Price histogram buckets: " + priceHist.getBuckets().size());
        System.out.println("Quantity histogram buckets: " + qtyHist.getBuckets().size());

        System.out.println("Multiple histograms test passed!");
    }

    // ==================== Sub-Aggregation Tests ====================

    @Test
    @DisplayName("Histogram with stats sub-aggregation")
    public void testHistogramWithSubAggregation() {
        System.out.println("Testing histogram with stats sub-aggregation...");

        SplitQuery query = new SplitMatchAllQuery();
        HistogramAggregation hist = new HistogramAggregation("price_hist", "price", 500.0)
            .addSubAggregation(new StatsAggregation("qty_stats", "quantity"));

        SearchResult result = searcher.search(query, 10, "price_hist", hist);

        HistogramResult histResult = (HistogramResult) result.getAggregation("price_hist");
        assertNotNull(histResult, "Histogram result should not be null");

        for (HistogramResult.HistogramBucket bucket : histResult.getBuckets()) {
            System.out.println("  Bucket " + bucket.getKey() + ": " + bucket.getDocCount() + " docs");

            // Check for sub-aggregations
            Map<String, AggregationResult> subAggs = bucket.getSubAggregations();
            if (subAggs != null && !subAggs.isEmpty()) {
                StatsResult stats = bucket.getSubAggregation("qty_stats", StatsResult.class);
                if (stats != null) {
                    System.out.println("    Stats - count: " + stats.getCount() +
                        ", sum: " + stats.getSum() + ", avg: " + stats.getAverage());
                }
            }
        }

        System.out.println("Histogram with sub-aggregation test passed!");
    }

    // ==================== JSON Generation Tests ====================

    @Test
    @DisplayName("Histogram JSON generation includes all parameters")
    public void testHistogramJsonGeneration() {
        System.out.println("Testing histogram JSON generation...");

        HistogramAggregation hist = new HistogramAggregation("test", "price", 100.0)
            .setOffset(25.0)
            .setMinDocCount(1)
            .setKeyed(true)
            .setHardBounds(0.0, 1000.0)
            .setExtendedBounds(0.0, 2000.0);

        String json = hist.toAggregationJson();
        System.out.println("Generated JSON: " + json);

        assertTrue(json.contains("\"histogram\""), "Should contain histogram type");
        assertTrue(json.contains("\"field\": \"price\""), "Should contain field name");
        assertTrue(json.contains("\"interval\": 100.0"), "Should contain interval");
        assertTrue(json.contains("\"offset\": 25.0"), "Should contain offset");
        assertTrue(json.contains("\"min_doc_count\": 1"), "Should contain min_doc_count");
        assertTrue(json.contains("\"keyed\": true"), "Should contain keyed");
        assertTrue(json.contains("\"hard_bounds\""), "Should contain hard_bounds");
        assertTrue(json.contains("\"extended_bounds\""), "Should contain extended_bounds");

        System.out.println("Histogram JSON generation test passed!");
    }

    @Test
    @DisplayName("Histogram JSON with sub-aggregation")
    public void testHistogramJsonWithSubAggregation() {
        System.out.println("Testing histogram JSON with sub-aggregation...");

        HistogramAggregation hist = new HistogramAggregation("test", "price", 100.0)
            .addSubAggregation(new StatsAggregation("nested_stats", "quantity"))
            .addSubAggregation(new AverageAggregation("nested_avg", "score"));

        String json = hist.toAggregationJson();
        System.out.println("Generated JSON: " + json);

        assertTrue(json.contains("\"aggs\""), "Should contain aggs section");
        assertTrue(json.contains("\"nested_stats\""), "Should contain nested_stats");
        assertTrue(json.contains("\"nested_avg\""), "Should contain nested_avg");
        assertTrue(json.contains("\"stats\""), "Should contain stats aggregation type");
        assertTrue(json.contains("\"avg\""), "Should contain avg aggregation type");

        System.out.println("Histogram JSON with sub-aggregation test passed!");
    }

    // ==================== Edge Cases ====================

    @Test
    @DisplayName("Histogram with very small interval")
    public void testHistogramVerySmallInterval() {
        System.out.println("Testing histogram with very small interval=1...");

        SplitQuery query = new SplitMatchAllQuery();
        HistogramAggregation hist = new HistogramAggregation("price_hist", "price", 1.0)
            .setMinDocCount(1);  // Only return buckets with documents

        SearchResult result = searcher.search(query, 10, "price_hist", hist);

        HistogramResult histResult = (HistogramResult) result.getAggregation("price_hist");
        assertNotNull(histResult, "Histogram result should not be null");

        // With min_doc_count=1 and interval=1, each unique price gets its own bucket
        // Prices: 10, 25, 50, 75, 100, 150, 200, 250, 500, 1000 = 10 unique values
        assertEquals(10, histResult.getBuckets().size(), "Should have 10 buckets (one per unique price)");

        // Verify all buckets have exactly 1 document
        for (HistogramResult.HistogramBucket bucket : histResult.getBuckets()) {
            assertEquals(1L, bucket.getDocCount(), "Each bucket should have exactly 1 doc");
        }

        System.out.println("Histogram with very small interval test passed!");
    }

    @Test
    @DisplayName("Histogram constructor validation")
    public void testHistogramConstructorValidation() {
        System.out.println("Testing histogram constructor validation...");

        // Test invalid interval
        assertThrows(IllegalArgumentException.class, () -> {
            new HistogramAggregation("test", "price", 0.0);
        }, "Should reject interval=0");

        assertThrows(IllegalArgumentException.class, () -> {
            new HistogramAggregation("test", "price", -100.0);
        }, "Should reject negative interval");

        // Test null/empty field name
        assertThrows(IllegalArgumentException.class, () -> {
            new HistogramAggregation("test", null, 100.0);
        }, "Should reject null field name");

        assertThrows(IllegalArgumentException.class, () -> {
            new HistogramAggregation("test", "", 100.0);
        }, "Should reject empty field name");

        System.out.println("Histogram constructor validation test passed!");
    }

    @Test
    @DisplayName("Histogram getters return correct values")
    public void testHistogramGetters() {
        System.out.println("Testing histogram getters...");

        HistogramAggregation hist = new HistogramAggregation("my_hist", "price", 100.0)
            .setOffset(25.0)
            .setMinDocCount(5)
            .setKeyed(true)
            .setHardBounds(0.0, 1000.0)
            .setExtendedBounds(0.0, 2000.0);

        assertEquals("my_hist", hist.getName(), "Name should match");
        assertEquals("price", hist.getFieldName(), "Field name should match");
        assertEquals(100.0, hist.getInterval(), 0.001, "Interval should match");
        assertEquals(25.0, hist.getOffset(), 0.001, "Offset should match");
        assertEquals(5L, hist.getMinDocCount(), "Min doc count should match");
        assertTrue(hist.isKeyed(), "Keyed should be true");
        assertNotNull(hist.getHardBounds(), "Hard bounds should not be null");
        assertEquals(0.0, hist.getHardBounds().min, 0.001, "Hard bounds min should match");
        assertEquals(1000.0, hist.getHardBounds().max, 0.001, "Hard bounds max should match");
        assertNotNull(hist.getExtendedBounds(), "Extended bounds should not be null");
        assertEquals(0.0, hist.getExtendedBounds().min, 0.001, "Extended bounds min should match");
        assertEquals(2000.0, hist.getExtendedBounds().max, 0.001, "Extended bounds max should match");

        System.out.println("Histogram getters test passed!");
    }

    // ==================== Additional Edge Cases ====================

    @Test
    @DisplayName("Histogram with query that returns no results")
    public void testHistogramEmptyResults() {
        System.out.println("Testing histogram with empty results...");

        SplitQuery query = new SplitTermQuery("category", "nonexistent");
        HistogramAggregation hist = new HistogramAggregation("price_hist", "price", 100.0);

        SearchResult result = searcher.search(query, 10, "price_hist", hist);

        assertEquals(0, result.getHits().size(), "Should have no search results");
        assertTrue(result.hasAggregations(), "Should still have aggregations");

        HistogramResult histResult = (HistogramResult) result.getAggregation("price_hist");
        assertNotNull(histResult, "Histogram result should not be null");

        // With empty results, histogram may have 0 buckets or empty buckets
        long totalDocs = 0;
        for (HistogramResult.HistogramBucket bucket : histResult.getBuckets()) {
            totalDocs += bucket.getDocCount();
        }
        assertEquals(0L, totalDocs, "Should have 0 total documents in histogram");

        System.out.println("Histogram empty results test passed!");
    }

    @Test
    @DisplayName("Histogram with very large interval (all docs in one bucket)")
    public void testHistogramLargeInterval() {
        System.out.println("Testing histogram with large interval...");

        SplitQuery query = new SplitMatchAllQuery();
        HistogramAggregation hist = new HistogramAggregation("price_hist", "price", 10000.0);

        SearchResult result = searcher.search(query, 10, "price_hist", hist);

        HistogramResult histResult = (HistogramResult) result.getAggregation("price_hist");
        assertNotNull(histResult, "Histogram result should not be null");

        // With interval=10000, all 10 docs (prices 10-1000) should be in one bucket
        assertEquals(1, histResult.getBuckets().size(), "Should have 1 bucket");

        HistogramResult.HistogramBucket bucket = histResult.getBuckets().get(0);
        assertEquals(10L, bucket.getDocCount(), "Single bucket should have all 10 docs");
        assertEquals(0.0, bucket.getKey(), 0.001, "Bucket key should be 0");

        System.out.println("Histogram large interval test passed!");
    }

    @Test
    @DisplayName("Histogram combined with terms aggregation")
    public void testHistogramWithTerms() {
        System.out.println("Testing histogram combined with terms aggregation...");

        SplitQuery query = new SplitMatchAllQuery();

        Map<String, SplitAggregation> aggs = new HashMap<>();
        aggs.put("price_hist", new HistogramAggregation("price_hist", "price", 500.0));
        aggs.put("categories", new TermsAggregation("category"));

        SearchResult result = searcher.search(query, 10, aggs);

        assertEquals(2, result.getAggregations().size(), "Should have 2 aggregations");

        HistogramResult hist = (HistogramResult) result.getAggregation("price_hist");
        TermsResult terms = (TermsResult) result.getAggregation("categories");

        assertNotNull(hist, "Histogram result should not be null");
        assertNotNull(terms, "Terms result should not be null");

        System.out.println("Histogram: " + hist.getBuckets().size() + " buckets");
        System.out.println("Terms: " + terms.getBuckets().size() + " categories");

        System.out.println("Histogram with terms test passed!");
    }

    @Test
    @DisplayName("Histogram bucket boundaries are correct")
    public void testHistogramBucketBoundaries() {
        System.out.println("Testing histogram bucket boundaries...");

        SplitQuery query = new SplitMatchAllQuery();
        // Using interval=100, expect buckets: 0, 100, 200, 500, 1000
        // Prices: 10, 25, 50, 75 (bucket 0), 100, 150 (bucket 100), 200, 250 (bucket 200), 500 (bucket 500), 1000 (bucket 1000)
        HistogramAggregation hist = new HistogramAggregation("price_hist", "price", 100.0)
            .setMinDocCount(1);

        SearchResult result = searcher.search(query, 10, "price_hist", hist);

        HistogramResult histResult = (HistogramResult) result.getAggregation("price_hist");
        assertNotNull(histResult, "Histogram result should not be null");

        // Verify buckets have correct keys (should be multiples of 100)
        for (HistogramResult.HistogramBucket bucket : histResult.getBuckets()) {
            double key = bucket.getKey();
            assertEquals(0.0, key % 100.0, 0.001, "Bucket key should be multiple of interval");
            System.out.println("  Bucket " + (long)key + ": " + bucket.getDocCount() + " docs");
        }

        System.out.println("Histogram bucket boundaries test passed!");
    }

    @Test
    @DisplayName("Histogram with exact boundary values")
    public void testHistogramExactBoundaryValues() {
        System.out.println("Testing histogram with exact boundary values...");

        SplitQuery query = new SplitMatchAllQuery();
        // Price 100 should go into bucket 100 (since lower bound is inclusive)
        HistogramAggregation hist = new HistogramAggregation("price_hist", "price", 100.0)
            .setMinDocCount(1);

        SearchResult result = searcher.search(query, 10, "price_hist", hist);

        HistogramResult histResult = (HistogramResult) result.getAggregation("price_hist");
        assertNotNull(histResult, "Histogram result should not be null");

        // Find the bucket with key=100
        HistogramResult.HistogramBucket bucket100 = null;
        for (HistogramResult.HistogramBucket bucket : histResult.getBuckets()) {
            if (Math.abs(bucket.getKey() - 100.0) < 0.001) {
                bucket100 = bucket;
                break;
            }
        }

        assertNotNull(bucket100, "Should have bucket with key=100");
        // Prices in [100, 200): 100, 150 = 2 docs
        assertEquals(2L, bucket100.getDocCount(), "Bucket 100 should have 2 docs (100, 150)");

        System.out.println("Histogram exact boundary values test passed!");
    }
}
