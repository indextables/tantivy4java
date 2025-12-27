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
import org.junit.jupiter.api.Disabled;
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
 * Comprehensive test suite for SplitSearcher bucket aggregation functionality.
 * Tests terms, range, date histogram, and histogram aggregations following
 * Quickwit's aggregation model.
 */
public class SplitSearcherBucketAggregationTest {

    private SplitCacheManager cacheManager;
    private SplitSearcher searcher;
    private String splitPath;
    private QuickwitSplit.SplitMetadata metadata;
    private String testName;

    @BeforeEach
    public void setUp(@TempDir Path tempDir, TestInfo testInfo) throws Exception {
        // Use unique test name for split path and cache to avoid conflicts between tests
        testName = testInfo.getTestMethod().get().getName();

        // Create test index with bucket aggregation data
        String indexPath = tempDir.resolve(testName + "_index").toString();
        splitPath = tempDir.resolve(testName + ".split").toString();

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder
                .addTextField("category", true, true, "default", "position")
                .addTextField("brand", true, true, "default", "position")
                .addTextField("status", true, true, "default", "position")
                .addIntegerField("price", true, true, true)
                .addIntegerField("rating", true, true, true)
                .addDateField("timestamp", true, true, true) // Date field for histogram
                .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        // Add test products with various categories, brands, prices
                        // For terms aggregation testing
                        addProduct(writer, 1, "electronics", "apple", "active", 1299, 5, 1672531200); // 2023-01-01
                        addProduct(writer, 2, "electronics", "samsung", "active", 899, 4, 1672617600); // 2023-01-02
                        addProduct(writer, 3, "books", "penguin", "active", 25, 4, 1672704000);        // 2023-01-03
                        addProduct(writer, 4, "books", "harper", "inactive", 30, 3, 1672790400);       // 2023-01-04
                        addProduct(writer, 5, "electronics", "apple", "active", 999, 5, 1672876800);  // 2023-01-05
                        addProduct(writer, 6, "clothing", "nike", "active", 120, 4, 1672963200);       // 2023-01-06
                        addProduct(writer, 7, "clothing", "adidas", "active", 95, 3, 1673049600);      // 2023-01-07
                        addProduct(writer, 8, "books", "penguin", "active", 35, 5, 1673136000);        // 2023-01-08

                        writer.commit();
                    }

                    // Convert to QuickwitSplit with unique split ID per test
                    QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                        "bucket-test-" + testName, "test-source", "test-node");

                    // Get the metadata with footer offsets from the conversion
                    QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);

                    // Store metadata for searcher creation
                    this.metadata = metadata;
                }
            }
        }

        // Create cache manager with unique name per test to avoid cache conflicts
        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig("bucket-test-cache-" + testName);
        cacheManager = SplitCacheManager.getInstance(cacheConfig);

        // Use the metadata with proper footer offsets from convertIndexFromPath
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

    private void addProduct(IndexWriter writer, int id, String category, String brand, String status,
                          int price, int rating, int timestamp) throws Exception {
        try (Document doc = new Document()) {
            doc.addInteger("id", id);
            doc.addText("category", category);
            doc.addText("brand", brand);
            doc.addText("status", status);
            doc.addInteger("price", price);
            doc.addInteger("rating", rating);
            // Convert Unix timestamp to LocalDateTime for date field
            LocalDateTime dateTime = LocalDateTime.ofInstant(
                Instant.ofEpochSecond(timestamp), ZoneId.systemDefault());
            doc.addDate("timestamp", dateTime);
            writer.addDocument(doc);
        }
    }

    @Test
    @DisplayName("Test terms aggregation - grouping by category")
    public void testTermsAggregation() {
        System.out.println("📊 Testing terms aggregation...");

        SplitQuery matchAllQuery = new SplitMatchAllQuery();
        TermsAggregation categoryTerms = new TermsAggregation("categories", "category", 10, 0);

        SearchResult result = searcher.search(matchAllQuery, 10, "terms", categoryTerms);

        assertTrue(result.hasAggregations(), "Result should contain aggregations");
        TermsResult terms = (TermsResult) result.getAggregation("terms");
        assertNotNull(terms, "Terms aggregation result should not be null");

        // Verify buckets: electronics (3), books (3), clothing (2) - actual aggregation results
        assertEquals(3, terms.getBuckets().size(), "Should have 3 category buckets");

        // Find specific buckets and verify counts
        Map<String, Long> bucketCounts = new HashMap<>();
        for (TermsResult.TermsBucket bucket : terms.getBuckets()) {
            bucketCounts.put(bucket.getKeyAsString(), bucket.getDocCount());
        }

        // Verify buckets: electronics (3), books (3), clothing (2) - based on actual data
        // Products: 1=electronics, 2=electronics, 3=books, 4=books, 5=electronics, 6=clothing, 7=clothing, 8=books
        assertEquals(3L, bucketCounts.get("electronics"), "Electronics should have 3 products");
        assertEquals(3L, bucketCounts.get("books"), "Books should have 3 products");
        assertEquals(2L, bucketCounts.get("clothing"), "Clothing should have 2 products");

        System.out.println("✅ Terms aggregation: " + terms);
        for (TermsResult.TermsBucket bucket : terms.getBuckets()) {
            System.out.println("  " + bucket);
        }
    }

    @Test
    @DisplayName("Test range aggregation - price ranges")
    public void testRangeAggregation() {
        System.out.println("💰 Testing range aggregation...");

        SplitQuery matchAllQuery = new SplitMatchAllQuery();
        RangeAggregation priceRanges = new RangeAggregation("price_ranges", "price")
            .addRange("budget", null, 50.0)       // < $50
            .addRange("mid", 50.0, 500.0)         // $50-$500
            .addRange("premium", 500.0, null);    // > $500

        SearchResult result = searcher.search(matchAllQuery, 10, "ranges", priceRanges);

        assertTrue(result.hasAggregations(), "Result should contain aggregations");

        // Debug: print available aggregations
        System.out.println("Available aggregations: " + result.getAggregations().keySet());

        RangeResult ranges = (RangeResult) result.getAggregation("ranges");
        assertNotNull(ranges, "Range aggregation result should not be null");

        assertEquals(3, ranges.getBuckets().size(), "Should have 3 price range buckets");

        // Verify range buckets
        Map<String, Long> rangeCounts = new HashMap<>();
        for (RangeResult.RangeBucket bucket : ranges.getBuckets()) {
            rangeCounts.put(bucket.getKey(), bucket.getDocCount());
        }

        // Budget: books (25, 30, 35) = 3 items
        assertEquals(3L, rangeCounts.get("budget"), "Budget range should have 3 items");
        // Mid: clothing (120, 95) = 2 items
        assertEquals(2L, rangeCounts.get("mid"), "Mid range should have 2 items");
        // Premium: electronics (1299, 899, 999) = 3 items
        assertEquals(3L, rangeCounts.get("premium"), "Premium range should have 3 items");

        System.out.println("✅ Range aggregation: " + ranges);
        for (RangeResult.RangeBucket bucket : ranges.getBuckets()) {
            System.out.println("  " + bucket);
        }
    }

    @Test
    @DisplayName("Test histogram aggregation - price distribution")
    public void testHistogramAggregation() {
        System.out.println("📈 Testing histogram aggregation...");

        SplitQuery matchAllQuery = new SplitMatchAllQuery();
        HistogramAggregation priceHist = new HistogramAggregation("price_dist", "price", 500.0);

        SearchResult result = searcher.search(matchAllQuery, 10, "histogram", priceHist);

        assertTrue(result.hasAggregations(), "Result should contain aggregations");
        HistogramResult hist = (HistogramResult) result.getAggregation("histogram");
        assertNotNull(hist, "Histogram aggregation result should not be null");

        // Should have buckets for different price intervals
        assertTrue(hist.getBuckets().size() >= 2, "Should have at least 2 histogram buckets");

        // Find buckets and verify
        Map<Double, Long> histCounts = new HashMap<>();
        for (HistogramResult.HistogramBucket bucket : hist.getBuckets()) {
            histCounts.put(bucket.getKey(), bucket.getDocCount());
        }

        // 0-500 range: books (25, 30, 35) + clothing (120, 95) = 5 items
        assertEquals(5L, histCounts.get(0.0), "0-500 bucket should have 5 items");
        // 500-1000 range: electronics (899, 999) = 2 items
        assertEquals(2L, histCounts.get(500.0), "500-1000 bucket should have 2 items");
        // 1000+ range: electronics (1299) = 1 item
        assertEquals(1L, histCounts.get(1000.0), "1000+ bucket should have 1 item");

        System.out.println("✅ Histogram aggregation: " + hist);
        for (HistogramResult.HistogramBucket bucket : hist.getBuckets()) {
            System.out.println("  " + bucket);
        }
    }

    @Test
    @DisplayName("Test date histogram aggregation - daily timeline")
    public void testDateHistogramAggregation() {
        System.out.println("📅 Testing date histogram aggregation...");

        SplitQuery matchAllQuery = new SplitMatchAllQuery();
        DateHistogramAggregation dailyHist = new DateHistogramAggregation("daily", "timestamp")
            .setFixedInterval("1d");

        SearchResult result = searcher.search(matchAllQuery, 10, "daily", dailyHist);

        assertTrue(result.hasAggregations(), "Result should contain aggregations");
        DateHistogramResult daily = (DateHistogramResult) result.getAggregation("daily");
        assertNotNull(daily, "Date histogram aggregation result should not be null");

        // Should have 8 daily buckets (one for each day)
        assertEquals(8, daily.getBuckets().size(), "Should have 8 daily buckets");

        // Verify each bucket has 1 document
        for (DateHistogramResult.DateHistogramBucket bucket : daily.getBuckets()) {
            assertEquals(1, bucket.getDocCount(), "Each daily bucket should have 1 document");
            assertNotNull(bucket.getKeyAsString(), "Bucket should have formatted date string");
        }

        System.out.println("✅ Date histogram aggregation: " + daily);
        for (DateHistogramResult.DateHistogramBucket bucket : daily.getBuckets()) {
            System.out.println("  " + bucket);
        }
    }

    @Test
    @DisplayName("Test multiple bucket aggregations together (Terms + Histogram)")
    public void testMultipleBucketAggregations() {
        System.out.println("🔢 Testing multiple bucket aggregations (Terms + Histogram)...");

        SplitQuery matchAllQuery = new SplitMatchAllQuery();

        // Test Terms + Histogram aggregations together
        // Note: RangeAggregation is not yet implemented in native layer
        Map<String, SplitAggregation> aggregations = new HashMap<>();
        aggregations.put("categories", new TermsAggregation("category"));
        aggregations.put("rating_dist", new HistogramAggregation("rating", 1.0));

        SearchResult result = searcher.search(matchAllQuery, 10, aggregations);

        assertTrue(result.hasAggregations(), "Result should contain aggregations");
        assertEquals(2, result.getAggregations().size(), "Should have 2 aggregations");

        // Verify all aggregations are present and working
        TermsResult categories = (TermsResult) result.getAggregation("categories");
        HistogramResult ratingDist = (HistogramResult) result.getAggregation("rating_dist");

        assertNotNull(categories, "Categories aggregation should not be null");
        assertNotNull(ratingDist, "Rating distribution should not be null");

        assertEquals(3, categories.getBuckets().size(), "Should have 3 categories");
        assertTrue(ratingDist.getBuckets().size() >= 2, "Should have rating distribution buckets");

        System.out.println("✅ Multiple bucket aggregations completed");
        System.out.println("   Categories: " + categories.getBuckets().size() + " buckets");
        System.out.println("   Rating distribution: " + ratingDist.getBuckets().size() + " buckets");
    }

    @Test
    @DisplayName("Test bucket aggregation JSON generation")
    public void testBucketAggregationJsonGeneration() {
        System.out.println("🔧 Testing bucket aggregation JSON generation...");

        // Test Terms aggregation JSON
        TermsAggregation termsAgg = new TermsAggregation("test_terms", "category", 5, 10);
        String termsJson = termsAgg.toAggregationJson();
        assertTrue(termsJson.contains("\"terms\""), "Should contain terms type");
        assertTrue(termsJson.contains("\"field\": \"category\""), "Should contain field name");
        assertTrue(termsJson.contains("\"size\": 5"), "Should contain size");
        assertTrue(termsJson.contains("\"shard_size\": 10"), "Should contain shard_size");

        // Test Range aggregation JSON
        RangeAggregation rangeAgg = new RangeAggregation("test_range", "price")
            .addRange("low", null, 100.0)
            .addRange("high", 100.0, null);
        String rangeJson = rangeAgg.toAggregationJson();
        assertTrue(rangeJson.contains("\"range\""), "Should contain range type");
        assertTrue(rangeJson.contains("\"ranges\""), "Should contain ranges array");

        // Test Histogram aggregation JSON
        HistogramAggregation histAgg = new HistogramAggregation("test_hist", "price", 50.0);
        String histJson = histAgg.toAggregationJson();
        assertTrue(histJson.contains("\"histogram\""), "Should contain histogram type");
        assertTrue(histJson.contains("\"interval\": 50.0"), "Should contain interval");

        // Test Date Histogram aggregation JSON
        DateHistogramAggregation dateHistAgg = new DateHistogramAggregation("test_date", "timestamp")
            .setFixedInterval("1d")
            .setOffset("-4h");
        String dateHistJson = dateHistAgg.toAggregationJson();
        assertTrue(dateHistJson.contains("\"date_histogram\""), "Should contain date_histogram type");
        assertTrue(dateHistJson.contains("\"fixed_interval\": \"1d\""), "Should contain fixed_interval");
        assertTrue(dateHistJson.contains("\"offset\": \"-4h\""), "Should contain offset");

        System.out.println("✅ All bucket aggregation JSON formats validated");
    }

    @Test
    @DisplayName("Test bucket aggregation error handling")
    public void testBucketAggregationErrorHandling() {
        System.out.println("⚠️ Testing bucket aggregation error handling...");

        // Test Terms aggregation errors
        assertThrows(IllegalArgumentException.class, () -> {
            new TermsAggregation(null);
        }, "Should throw exception for null field name");

        assertThrows(IllegalArgumentException.class, () -> {
            new TermsAggregation("field", 0);
        }, "Should throw exception for zero size");

        // Test Range aggregation errors
        RangeAggregation rangeAgg = new RangeAggregation("test", "field");
        assertThrows(IllegalStateException.class, () -> {
            rangeAgg.toAggregationJson();
        }, "Should throw exception for empty ranges");

        // Test Histogram aggregation errors
        assertThrows(IllegalArgumentException.class, () -> {
            new HistogramAggregation("field", 0.0);
        }, "Should throw exception for zero interval");

        // Test Date Histogram aggregation errors
        DateHistogramAggregation dateHistAgg = new DateHistogramAggregation("test", "field");
        assertThrows(IllegalStateException.class, () -> {
            dateHistAgg.toAggregationJson();
        }, "Should throw exception for missing interval");

        System.out.println("✅ Error handling validated");
    }
}
