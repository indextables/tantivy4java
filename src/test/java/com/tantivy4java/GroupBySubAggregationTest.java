package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.Map;
import java.util.HashMap;

/**
 * Test GROUP BY with sub-aggregations functionality as requested in the feature request.
 * This tests the exact usage pattern from the Spark DataSource V2 integration.
 */
public class GroupBySubAggregationTest {

    private SplitCacheManager cacheManager;
    private SplitSearcher searcher;
    private String splitPath;
    private QuickwitSplit.SplitMetadata metadata;

    @BeforeEach
    public void setUp(@TempDir Path tempDir) throws Exception {
        String uniqueId = "groupby_subagg_test_" + System.nanoTime();
        String indexPath = tempDir.resolve(uniqueId + "_index").toString();
        splitPath = tempDir.resolve(uniqueId + ".split").toString();

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder
                .addTextField("category", true, true, "default", "position")  // Group by field - FAST FIELD REQUIRED
                .addIntegerField("sales_amount", true, true, true)  // Sum this
                .addIntegerField("price", true, true, true)         // Avg this
                .addIntegerField("score", true, true, true);        // Max this

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        // Add test data for GROUP BY scenario
                        // Electronics category
                        try (Document doc1 = new Document()) {
                            doc1.addText("category", "electronics");
                            doc1.addInteger("sales_amount", 1000);
                            doc1.addInteger("price", 500);
                            doc1.addInteger("score", 85);
                            writer.addDocument(doc1);
                        }

                        try (Document doc2 = new Document()) {
                            doc2.addText("category", "electronics");
                            doc2.addInteger("sales_amount", 1500);
                            doc2.addInteger("price", 750);
                            doc2.addInteger("score", 90);
                            writer.addDocument(doc2);
                        }

                        // Books category
                        try (Document doc3 = new Document()) {
                            doc3.addText("category", "books");
                            doc3.addInteger("sales_amount", 500);
                            doc3.addInteger("price", 25);
                            doc3.addInteger("score", 70);
                            writer.addDocument(doc3);
                        }

                        try (Document doc4 = new Document()) {
                            doc4.addText("category", "books");
                            doc4.addInteger("sales_amount", 300);
                            doc4.addInteger("price", 15);
                            doc4.addInteger("score", 75);
                            writer.addDocument(doc4);
                        }

                        writer.commit();
                    }

                    QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                        uniqueId, "test-source", "test-node");

                    metadata = QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);
                    this.metadata = metadata;
                }
            }
        }

        String uniqueCacheName = uniqueId + "-cache";
        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig(uniqueCacheName);
        cacheManager = SplitCacheManager.getInstance(cacheConfig);

        searcher = cacheManager.createSplitSearcher("file://" + splitPath, metadata);
    }

    @AfterEach
    public void tearDown() {
        if (searcher != null) {
            searcher.close();
        }
    }

    @Test
    public void testGroupByWithSubAggregations() {
        System.out.println("🚀 Testing GROUP BY with sub-aggregations (Spark DataSource V2 pattern)...");

        // Create GROUP BY aggregation with sub-aggregations (as specified in feature request)
        TermsAggregation groupBy = new TermsAggregation("category_field", "category", 10, 0);
        groupBy.addSubAggregation("total_sales", new SumAggregation("sales_amount"));
        groupBy.addSubAggregation("avg_price", new AverageAggregation("price"));
        groupBy.addSubAggregation("max_score", new MaxAggregation("score"));

        SplitQuery matchAllQuery = new SplitMatchAllQuery();

        System.out.println("📊 GROUP BY aggregation: " + groupBy);
        System.out.println("📊 Generated JSON: " + groupBy.toAggregationJson());

        // Execute search with GROUP BY + sub-aggregations
        SearchResult result = searcher.search(matchAllQuery, 1000, "group_agg", groupBy);

        System.out.println("🔍 hasAggregations(): " + result.hasAggregations());
        assertTrue(result.hasAggregations(), "Should have aggregations");

        // Get the terms result
        TermsResult termsResult = (TermsResult) result.getAggregation("group_agg");
        assertNotNull(termsResult, "Terms result should not be null");

        System.out.println("📊 Terms result: " + termsResult);
        System.out.println("📊 Number of buckets: " + termsResult.getBuckets().size());

        // Validate buckets
        assertTrue(termsResult.getBuckets().size() >= 2, "Should have at least 2 category buckets");

        // Test sub-aggregation access within buckets (THE MAIN FEATURE)
        for (TermsResult.TermsBucket bucket : termsResult.getBuckets()) {
            String category = bucket.getKeyAsString();
            long docCount = bucket.getDocCount();

            System.out.println("📦 Bucket: " + category + " (count=" + docCount + ")");

            // This is the core feature from the request
            SumResult totalSales = (SumResult) bucket.getSubAggregation("total_sales");
            AverageResult avgPrice = (AverageResult) bucket.getSubAggregation("avg_price");
            MaxResult maxScore = (MaxResult) bucket.getSubAggregation("max_score");

            if (totalSales != null) {
                System.out.println("  💰 Total sales: " + totalSales.getSum());
            } else {
                System.out.println("  ❌ Total sales: null");
            }

            if (avgPrice != null) {
                System.out.println("  💵 Average price: " + avgPrice.getAverage());
            } else {
                System.out.println("  ❌ Average price: null");
            }

            if (maxScore != null) {
                System.out.println("  ⭐ Max score: " + maxScore.getMax());
            } else {
                System.out.println("  ❌ Max score: null");
            }

            // Validate sub-aggregations exist
            assertTrue(bucket.hasSubAggregations(), "Bucket should have sub-aggregations");
            assertNotNull(totalSales, "Total sales sub-aggregation should not be null");
            assertNotNull(avgPrice, "Average price sub-aggregation should not be null");
            assertNotNull(maxScore, "Max score sub-aggregation should not be null");

            // Validate specific values for expected categories
            if ("electronics".equals(category)) {
                assertEquals(2, docCount, "Electronics should have 2 documents");
                assertEquals(2500.0, totalSales.getSum(), 0.01, "Electronics total sales should be 2500");
                assertEquals(625.0, avgPrice.getAverage(), 0.01, "Electronics avg price should be 625");
                assertEquals(90.0, maxScore.getMax(), 0.01, "Electronics max score should be 90");
            } else if ("books".equals(category)) {
                assertEquals(2, docCount, "Books should have 2 documents");
                assertEquals(800.0, totalSales.getSum(), 0.01, "Books total sales should be 800");
                assertEquals(20.0, avgPrice.getAverage(), 0.01, "Books avg price should be 20");
                assertEquals(75.0, maxScore.getMax(), 0.01, "Books max score should be 75");
            }
        }

        System.out.println("✅ GROUP BY with sub-aggregations test completed successfully!");
    }

    @Test
    public void testGeneratedNestedAggregationJson() {
        System.out.println("🧪 Testing nested aggregation JSON generation...");

        // Create GROUP BY with sub-aggregations
        TermsAggregation groupBy = new TermsAggregation("category_group", "category", 5, 0);
        groupBy.addSubAggregation("sum_sales", new SumAggregation("sales_amount"));
        groupBy.addSubAggregation("avg_price", new AverageAggregation("price"));

        String json = groupBy.toAggregationJson();
        System.out.println("📊 Generated JSON: " + json);

        // Verify JSON structure matches expected format from feature request
        assertTrue(json.contains("\"terms\""), "Should contain terms aggregation");
        assertTrue(json.contains("\"field\": \"category\""), "Should contain category field");
        assertTrue(json.contains("\"aggs\""), "Should contain aggs section for sub-aggregations");
        assertTrue(json.contains("\"sum_sales\""), "Should contain sum_sales sub-aggregation");
        assertTrue(json.contains("\"avg_price\""), "Should contain avg_price sub-aggregation");
        assertTrue(json.contains("\"sum\""), "Should contain sum aggregation type");
        assertTrue(json.contains("\"avg\""), "Should contain avg aggregation type");

        System.out.println("✅ Nested aggregation JSON generation verified!");
    }
}