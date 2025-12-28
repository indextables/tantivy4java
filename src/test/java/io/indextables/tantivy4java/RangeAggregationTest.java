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
import java.time.ZoneOffset;

/**
 * Comprehensive test suite for RangeAggregation functionality.
 * Tests all range aggregation features including:
 * - Named and unbounded ranges
 * - Keyed output format
 * - Sub-aggregations
 * - Edge cases
 */
public class RangeAggregationTest {

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
                .addFloatField("rating", true, true, true)
                .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Create test data with specific distribution for range testing
                        // Prices: 10, 25, 50, 75, 100, 150, 200, 300, 500, 1000
                        addProduct(writer, 1, "electronics", "phone", 10, 5, 4.5f);
                        addProduct(writer, 2, "electronics", "tablet", 25, 3, 4.0f);
                        addProduct(writer, 3, "electronics", "laptop", 50, 2, 3.5f);
                        addProduct(writer, 4, "books", "novel", 75, 10, 4.2f);
                        addProduct(writer, 5, "books", "textbook", 100, 8, 3.8f);
                        addProduct(writer, 6, "clothing", "shirt", 150, 20, 4.0f);
                        addProduct(writer, 7, "clothing", "pants", 200, 15, 3.5f);
                        addProduct(writer, 8, "clothing", "jacket", 300, 5, 4.5f);
                        addProduct(writer, 9, "electronics", "tv", 500, 2, 4.8f);
                        addProduct(writer, 10, "electronics", "camera", 1000, 1, 5.0f);

                        writer.commit();
                    }

                    QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                        "range-test-" + testName, "test-source", "test-node");

                    metadata = QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);
                }
            }
        }

        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig("range-test-cache-" + testName);
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
                           int price, int quantity, float rating) throws Exception {
        try (Document doc = new Document()) {
            doc.addInteger("id", id);
            doc.addText("category", category);
            doc.addText("name", name);
            doc.addInteger("price", price);
            doc.addInteger("quantity", quantity);
            doc.addFloat("rating", rating);
            writer.addDocument(doc);
        }
    }

    // ==================== Basic Range Tests ====================

    @Test
    @DisplayName("Basic range aggregation with named ranges")
    public void testBasicRangeAggregation() {
        System.out.println("Testing basic range aggregation...");

        SplitQuery query = new SplitMatchAllQuery();
        RangeAggregation range = new RangeAggregation("price_ranges", "price")
            .addRange("cheap", null, 100.0)       // < 100
            .addRange("medium", 100.0, 500.0)     // 100-500
            .addRange("expensive", 500.0, null);  // >= 500

        SearchResult result = searcher.search(query, 10, "price_ranges", range);

        assertTrue(result.hasAggregations(), "Should have aggregations");
        RangeResult rangeResult = (RangeResult) result.getAggregation("price_ranges");
        assertNotNull(rangeResult, "Range result should not be null");

        assertEquals(3, rangeResult.getBuckets().size(), "Should have 3 range buckets");

        Map<String, Long> rangeCounts = new HashMap<>();
        for (RangeResult.RangeBucket bucket : rangeResult.getBuckets()) {
            rangeCounts.put(bucket.getKey(), bucket.getDocCount());
            System.out.println("  " + bucket);
        }

        // < 100: prices 10, 25, 50, 75 = 4 docs
        assertEquals(4L, rangeCounts.get("cheap"), "Cheap range should have 4 docs");
        // 100-500: prices 100, 150, 200, 300 = 4 docs
        assertEquals(4L, rangeCounts.get("medium"), "Medium range should have 4 docs");
        // >= 500: prices 500, 1000 = 2 docs
        assertEquals(2L, rangeCounts.get("expensive"), "Expensive range should have 2 docs");

        System.out.println("Basic range aggregation test passed!");
    }

    @Test
    @DisplayName("Range aggregation with only lower bound")
    public void testRangeWithOnlyLowerBound() {
        System.out.println("Testing range with only lower bound...");

        SplitQuery query = new SplitMatchAllQuery();
        RangeAggregation range = new RangeAggregation("high_price", "price")
            .addRange("high", 200.0, null);  // >= 200

        SearchResult result = searcher.search(query, 10, "high_price", range);

        RangeResult rangeResult = (RangeResult) result.getAggregation("high_price");
        assertNotNull(rangeResult, "Range result should not be null");
        assertTrue(rangeResult.getBuckets().size() >= 1, "Should have at least 1 range bucket");

        // Find the "high" bucket
        RangeResult.RangeBucket highBucket = null;
        for (RangeResult.RangeBucket bucket : rangeResult.getBuckets()) {
            System.out.println("  " + bucket);
            if ("high".equals(bucket.getKey())) {
                highBucket = bucket;
            }
        }

        assertNotNull(highBucket, "Should have 'high' bucket");
        assertEquals(200.0, highBucket.getFrom(), "From should be 200");
        assertNull(highBucket.getTo(), "To should be null");
        // Prices >= 200: 200, 300, 500, 1000 = 4 docs
        assertEquals(4L, highBucket.getDocCount(), "Should have 4 docs");

        System.out.println("Range with only lower bound test passed!");
    }

    @Test
    @DisplayName("Range aggregation with only upper bound")
    public void testRangeWithOnlyUpperBound() {
        System.out.println("Testing range with only upper bound...");

        SplitQuery query = new SplitMatchAllQuery();
        RangeAggregation range = new RangeAggregation("low_price", "price")
            .addRange("low", null, 100.0);  // < 100

        SearchResult result = searcher.search(query, 10, "low_price", range);

        RangeResult rangeResult = (RangeResult) result.getAggregation("low_price");
        assertNotNull(rangeResult, "Range result should not be null");
        assertTrue(rangeResult.getBuckets().size() >= 1, "Should have at least 1 range bucket");

        // Find the "low" bucket
        RangeResult.RangeBucket lowBucket = null;
        for (RangeResult.RangeBucket bucket : rangeResult.getBuckets()) {
            System.out.println("  " + bucket);
            if ("low".equals(bucket.getKey())) {
                lowBucket = bucket;
            }
        }

        assertNotNull(lowBucket, "Should have 'low' bucket");
        assertNull(lowBucket.getFrom(), "From should be null");
        assertEquals(100.0, lowBucket.getTo(), "To should be 100");
        // Prices < 100: 10, 25, 50, 75 = 4 docs
        assertEquals(4L, lowBucket.getDocCount(), "Should have 4 docs");

        System.out.println("Range with only upper bound test passed!");
    }

    @Test
    @DisplayName("Range aggregation with many ranges")
    public void testRangeWithManyRanges() {
        System.out.println("Testing range with many ranges...");

        SplitQuery query = new SplitMatchAllQuery();
        RangeAggregation range = new RangeAggregation("detailed_prices", "price")
            .addRange("0-50", null, 50.0)
            .addRange("50-100", 50.0, 100.0)
            .addRange("100-200", 100.0, 200.0)
            .addRange("200-500", 200.0, 500.0)
            .addRange("500+", 500.0, null);

        SearchResult result = searcher.search(query, 10, "detailed_prices", range);

        RangeResult rangeResult = (RangeResult) result.getAggregation("detailed_prices");
        assertNotNull(rangeResult, "Range result should not be null");
        assertEquals(5, rangeResult.getBuckets().size(), "Should have 5 range buckets");

        long totalDocs = 0;
        for (RangeResult.RangeBucket bucket : rangeResult.getBuckets()) {
            totalDocs += bucket.getDocCount();
            System.out.println("  " + bucket);
        }
        assertEquals(10L, totalDocs, "Total docs should be 10");

        System.out.println("Range with many ranges test passed!");
    }

    // ==================== Combined with Query Filter ====================

    @Test
    @DisplayName("Range aggregation filtered by category")
    public void testRangeWithQueryFilter() {
        System.out.println("Testing range with query filter...");

        SplitQuery query = new SplitTermQuery("category", "electronics");
        RangeAggregation range = new RangeAggregation("electronics_prices", "price")
            .addRange("budget", null, 100.0)
            .addRange("premium", 100.0, null);

        SearchResult result = searcher.search(query, 10, "electronics_prices", range);

        RangeResult rangeResult = (RangeResult) result.getAggregation("electronics_prices");
        assertNotNull(rangeResult, "Range result should not be null");

        Map<String, Long> rangeCounts = new HashMap<>();
        for (RangeResult.RangeBucket bucket : rangeResult.getBuckets()) {
            rangeCounts.put(bucket.getKey(), bucket.getDocCount());
            System.out.println("  " + bucket);
        }

        // Electronics < 100: phone(10), tablet(25), laptop(50) = 3 docs
        assertEquals(3L, rangeCounts.get("budget"), "Budget electronics should have 3 docs");
        // Electronics >= 100: tv(500), camera(1000) = 2 docs
        assertEquals(2L, rangeCounts.get("premium"), "Premium electronics should have 2 docs");

        System.out.println("Range with query filter test passed!");
    }

    // ==================== Different Field Types ====================

    @Test
    @DisplayName("Range aggregation on quantity field")
    public void testRangeOnQuantityField() {
        System.out.println("Testing range on quantity field...");

        SplitQuery query = new SplitMatchAllQuery();
        RangeAggregation range = new RangeAggregation("stock_levels", "quantity")
            .addRange("low_stock", null, 5.0)
            .addRange("medium_stock", 5.0, 15.0)
            .addRange("high_stock", 15.0, null);

        SearchResult result = searcher.search(query, 10, "stock_levels", range);

        RangeResult rangeResult = (RangeResult) result.getAggregation("stock_levels");
        assertNotNull(rangeResult, "Range result should not be null");

        long totalDocs = 0;
        for (RangeResult.RangeBucket bucket : rangeResult.getBuckets()) {
            totalDocs += bucket.getDocCount();
            System.out.println("  " + bucket);
        }
        assertEquals(10L, totalDocs, "Total docs should be 10");

        System.out.println("Range on quantity field test passed!");
    }

    @Test
    @DisplayName("Range aggregation on float rating field")
    public void testRangeOnFloatField() {
        System.out.println("Testing range on float rating field...");

        SplitQuery query = new SplitMatchAllQuery();
        RangeAggregation range = new RangeAggregation("ratings", "rating")
            .addRange("low", null, 4.0)
            .addRange("medium", 4.0, 4.5)
            .addRange("high", 4.5, null);

        SearchResult result = searcher.search(query, 10, "ratings", range);

        RangeResult rangeResult = (RangeResult) result.getAggregation("ratings");
        assertNotNull(rangeResult, "Range result should not be null");

        long totalDocs = 0;
        for (RangeResult.RangeBucket bucket : rangeResult.getBuckets()) {
            totalDocs += bucket.getDocCount();
            System.out.println("  " + bucket);
        }
        assertEquals(10L, totalDocs, "Total docs should be 10");

        System.out.println("Range on float field test passed!");
    }

    // ==================== Multiple Aggregations ====================

    @Test
    @DisplayName("Multiple range aggregations in one query")
    public void testMultipleRangeAggregations() {
        System.out.println("Testing multiple range aggregations...");

        SplitQuery query = new SplitMatchAllQuery();

        Map<String, SplitAggregation> aggs = new HashMap<>();
        aggs.put("price_ranges", new RangeAggregation("price_ranges", "price")
            .addRange("low", null, 100.0)
            .addRange("high", 100.0, null));
        aggs.put("stock_ranges", new RangeAggregation("stock_ranges", "quantity")
            .addRange("low", null, 10.0)
            .addRange("high", 10.0, null));

        SearchResult result = searcher.search(query, 10, aggs);

        assertTrue(result.hasAggregations(), "Should have aggregations");
        assertEquals(2, result.getAggregations().size(), "Should have 2 aggregations");

        RangeResult priceRanges = (RangeResult) result.getAggregation("price_ranges");
        RangeResult stockRanges = (RangeResult) result.getAggregation("stock_ranges");

        assertNotNull(priceRanges, "Price ranges should not be null");
        assertNotNull(stockRanges, "Stock ranges should not be null");

        System.out.println("Price ranges: " + priceRanges.getBuckets().size() + " buckets");
        System.out.println("Stock ranges: " + stockRanges.getBuckets().size() + " buckets");

        System.out.println("Multiple range aggregations test passed!");
    }

    // ==================== Range with Other Aggregation Types ====================

    @Test
    @DisplayName("Range combined with terms aggregation")
    public void testRangeWithTerms() {
        System.out.println("Testing range combined with terms aggregation...");

        SplitQuery query = new SplitMatchAllQuery();

        Map<String, SplitAggregation> aggs = new HashMap<>();
        aggs.put("price_ranges", new RangeAggregation("price_ranges", "price")
            .addRange("affordable", null, 200.0)
            .addRange("expensive", 200.0, null));
        aggs.put("categories", new TermsAggregation("category"));

        SearchResult result = searcher.search(query, 10, aggs);

        assertEquals(2, result.getAggregations().size(), "Should have 2 aggregations");

        RangeResult ranges = (RangeResult) result.getAggregation("price_ranges");
        TermsResult terms = (TermsResult) result.getAggregation("categories");

        assertNotNull(ranges, "Range result should not be null");
        assertNotNull(terms, "Terms result should not be null");

        System.out.println("Ranges: " + ranges);
        System.out.println("Terms: " + terms);

        System.out.println("Range with terms test passed!");
    }

    // ==================== JSON Generation Tests ====================

    @Test
    @DisplayName("Range JSON generation includes all parameters")
    public void testRangeJsonGeneration() {
        System.out.println("Testing range JSON generation...");

        RangeAggregation range = new RangeAggregation("test", "price")
            .addRange("cheap", null, 100.0)
            .addRange("medium", 100.0, 500.0)
            .addRange("expensive", 500.0, null);

        String json = range.toAggregationJson();
        System.out.println("Generated JSON: " + json);

        assertTrue(json.contains("\"range\""), "Should contain range type");
        assertTrue(json.contains("\"field\": \"price\""), "Should contain field name");
        assertTrue(json.contains("\"ranges\""), "Should contain ranges array");
        assertTrue(json.contains("\"key\": \"cheap\""), "Should contain cheap key");

        System.out.println("Range JSON generation test passed!");
    }

    // ==================== Validation Tests ====================

    @Test
    @DisplayName("Range constructor validation")
    public void testRangeConstructorValidation() {
        System.out.println("Testing range constructor validation...");

        // Test null field name
        assertThrows(IllegalArgumentException.class, () -> {
            new RangeAggregation("test", null);
        }, "Should reject null field name");

        assertThrows(IllegalArgumentException.class, () -> {
            new RangeAggregation("test", "");
        }, "Should reject empty field name");

        System.out.println("Range constructor validation test passed!");
    }

    @Test
    @DisplayName("Range getters return correct values")
    public void testRangeGetters() {
        System.out.println("Testing range getters...");

        RangeAggregation range = new RangeAggregation("my_range", "price")
            .addRange("low", null, 100.0)
            .addRange("high", 100.0, null);

        assertEquals("my_range", range.getName(), "Name should match");
        assertEquals("price", range.getFieldName(), "Field name should match");
        assertEquals(2, range.getRanges().size(), "Should have 2 ranges");

        // Test RangeSpec getters
        RangeAggregation.RangeSpec lowSpec = range.getRanges().get(0);
        assertEquals("low", lowSpec.getKey(), "Key should be 'low'");
        assertNull(lowSpec.getFrom(), "From should be null");
        assertEquals(100.0, lowSpec.getTo(), "To should be 100");

        RangeAggregation.RangeSpec highSpec = range.getRanges().get(1);
        assertEquals("high", highSpec.getKey(), "Key should be 'high'");
        assertEquals(100.0, highSpec.getFrom(), "From should be 100");
        assertNull(highSpec.getTo(), "To should be null");

        System.out.println("Range getters test passed!");
    }

    // ==================== Edge Cases ====================

    @Test
    @DisplayName("Range with single unbounded range (match all)")
    public void testRangeMatchAll() {
        System.out.println("Testing range with unbounded range (match all)...");

        SplitQuery query = new SplitMatchAllQuery();
        RangeAggregation range = new RangeAggregation("all_prices", "price")
            .addRange("all", null, null);  // Matches everything

        SearchResult result = searcher.search(query, 10, "all_prices", range);

        RangeResult rangeResult = (RangeResult) result.getAggregation("all_prices");
        assertNotNull(rangeResult, "Range result should not be null");
        assertEquals(1, rangeResult.getBuckets().size(), "Should have 1 bucket");

        RangeResult.RangeBucket bucket = rangeResult.getBuckets().get(0);
        assertEquals(10L, bucket.getDocCount(), "Should match all 10 docs");
        assertNull(bucket.getFrom(), "From should be null");
        assertNull(bucket.getTo(), "To should be null");

        System.out.println("Range match all test passed!");
    }

    // Note: Overlapping ranges are NOT supported by Quickwit/Tantivy
    // See error: "Overlapping ranges not supported"

    @Test
    @DisplayName("Range with empty result bucket")
    public void testRangeWithEmptyBucket() {
        System.out.println("Testing range with empty bucket...");

        SplitQuery query = new SplitMatchAllQuery();
        RangeAggregation range = new RangeAggregation("with_empty", "price")
            .addRange("very_cheap", null, 5.0)     // No docs < 5
            .addRange("normal", 5.0, 100.0)
            .addRange("expensive", 100.0, null);

        SearchResult result = searcher.search(query, 10, "with_empty", range);

        RangeResult rangeResult = (RangeResult) result.getAggregation("with_empty");
        assertNotNull(rangeResult, "Range result should not be null");

        Map<String, Long> rangeCounts = new HashMap<>();
        for (RangeResult.RangeBucket bucket : rangeResult.getBuckets()) {
            rangeCounts.put(bucket.getKey(), bucket.getDocCount());
            System.out.println("  " + bucket);
        }

        // very_cheap range should have 0 docs (or might not be returned at all)
        Long veryCheapCount = rangeCounts.get("very_cheap");
        if (veryCheapCount != null) {
            assertEquals(0L, veryCheapCount, "Very cheap range should have 0 docs");
        }

        System.out.println("Range with empty bucket test passed!");
    }
}
