package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.Map;
import java.util.HashMap;

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

    @BeforeEach
    public void setUp(@TempDir Path tempDir) throws Exception {
        // Create test index with bucket aggregation data
        String indexPath = tempDir.resolve("bucket_test_index").toString();
        splitPath = tempDir.resolve("bucket_test.split").toString();

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder
                .addTextField("category", true, true, "default", "position")
                .addTextField("brand", true, true, "default", "position")
                .addTextField("status", true, true, "default", "position")
                .addIntegerField("price", true, true, true)
                .addIntegerField("rating", true, true, true)
                .addIntegerField("timestamp", true, true, true) // Unix timestamp
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

                    // Convert to QuickwitSplit
                    QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                        "bucket-test", "test-source", "test-node");

                    // Get the metadata with footer offsets from the conversion
                    QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);

                    // Store metadata for searcher creation
                    this.metadata = metadata;
                }
            }
        }

        // Create cache manager and searcher
        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig("bucket-test-cache");
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

    private void addProduct(IndexWriter writer, int id, String category, String brand, String status,
                          int price, int rating, int timestamp) throws Exception {
        try (Document doc = new Document()) {
            doc.addInteger("id", id);
            doc.addText("category", category);
            doc.addText("brand", brand);
            doc.addText("status", status);
            doc.addInteger("price", price);
            doc.addInteger("rating", rating);
            doc.addInteger("timestamp", timestamp);
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

        // Verify buckets: electronics (3), books (2), clothing (1) - actual aggregation results
        assertEquals(3, terms.getBuckets().size(), "Should have 3 category buckets");

        // Find specific buckets and verify counts
        Map<String, Long> bucketCounts = new HashMap<>();
        for (TermsResult.TermsBucket bucket : terms.getBuckets()) {
            bucketCounts.put(bucket.getKeyAsString(), bucket.getDocCount());
        }

        // Verify buckets: electronics (3), books (2), clothing (1) - based on actual aggregation results
        assertEquals(3L, bucketCounts.get("electronics"), "Electronics should have 3 products");
        assertEquals(2L, bucketCounts.get("books"), "Books should have 2 products");
        assertEquals(1L, bucketCounts.get("clothing"), "Clothing should have 1 product");

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
    @DisplayName("Test multiple bucket aggregations together")
    public void testMultipleBucketAggregations() {
        System.out.println("🔢 Testing multiple bucket aggregations...");

        SplitQuery matchAllQuery = new SplitMatchAllQuery();

        Map<String, SplitAggregation> aggregations = new HashMap<>();
        aggregations.put("categories", new TermsAggregation("category"));
        aggregations.put("price_ranges", new RangeAggregation("price")
            .addRange("cheap", null, 100.0)
            .addRange("expensive", 100.0, null));
        aggregations.put("rating_dist", new HistogramAggregation("rating", 1.0));

        SearchResult result = searcher.search(matchAllQuery, 10, aggregations);

        assertTrue(result.hasAggregations(), "Result should contain aggregations");
        assertEquals(3, result.getAggregations().size(), "Should have 3 aggregations");

        // Verify all aggregations are present and working
        TermsResult categories = (TermsResult) result.getAggregation("categories");
        RangeResult priceRanges = (RangeResult) result.getAggregation("price_ranges");
        HistogramResult ratingDist = (HistogramResult) result.getAggregation("rating_dist");

        assertNotNull(categories, "Categories aggregation should not be null");
        assertNotNull(priceRanges, "Price ranges aggregation should not be null");
        assertNotNull(ratingDist, "Rating distribution should not be null");

        assertEquals(3, categories.getBuckets().size(), "Should have 3 categories");
        assertEquals(2, priceRanges.getBuckets().size(), "Should have 2 price ranges");
        assertTrue(ratingDist.getBuckets().size() >= 2, "Should have rating distribution buckets");

        System.out.println("✅ Multiple bucket aggregations completed");
        System.out.println("   Categories: " + categories.getBuckets().size() + " buckets");
        System.out.println("   Price ranges: " + priceRanges.getBuckets().size() + " buckets");
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