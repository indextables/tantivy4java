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
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.Map;

/**
 * Comprehensive test for multi-dimensional GROUP BY functionality using nested aggregations.
 * Tests the exact usage pattern from the feature request.
 */
public class MultiTermsAggregationTest {

    private SplitCacheManager cacheManager;
    private SplitSearcher searcher;
    private String splitPath;
    private QuickwitSplit.SplitMetadata metadata;

    @BeforeEach
    public void setUp(@TempDir Path tempDir) throws Exception {
        String uniqueId = "multi_terms_test_" + System.nanoTime();
        String indexPath = tempDir.resolve(uniqueId + "_index").toString();
        splitPath = tempDir.resolve(uniqueId + ".split").toString();

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder
                .addTextField("region", true, true, "default", "position")     // Fast field for aggregation
                .addTextField("category", true, true, "default", "position")   // Fast field for aggregation
                .addTextField("quarter", true, true, "default", "position")    // Fast field for aggregation
                .addIntegerField("sales_amount", true, true, true)             // Sum this
                .addIntegerField("price", true, true, true)                    // Avg this
                .addIntegerField("rating", true, true, true);                  // Max this

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        // Test data for multi-dimensional GROUP BY
                        // Region: North/South, Category: Electronics/Books, Quarter: Q1/Q2

                        // North + Electronics + Q1
                        try (Document doc1 = new Document()) {
                            doc1.addText("region", "North");
                            doc1.addText("category", "Electronics");
                            doc1.addText("quarter", "Q1");
                            doc1.addInteger("sales_amount", 1000);
                            doc1.addInteger("price", 500);
                            doc1.addInteger("rating", 85);
                            writer.addDocument(doc1);
                        }

                        try (Document doc2 = new Document()) {
                            doc2.addText("region", "North");
                            doc2.addText("category", "Electronics");
                            doc2.addText("quarter", "Q1");
                            doc2.addInteger("sales_amount", 1500);
                            doc2.addInteger("price", 750);
                            doc2.addInteger("rating", 90);
                            writer.addDocument(doc2);
                        }

                        // North + Electronics + Q2
                        try (Document doc3 = new Document()) {
                            doc3.addText("region", "North");
                            doc3.addText("category", "Electronics");
                            doc3.addText("quarter", "Q2");
                            doc3.addInteger("sales_amount", 800);
                            doc3.addInteger("price", 400);
                            doc3.addInteger("rating", 80);
                            writer.addDocument(doc3);
                        }

                        // North + Books + Q1
                        try (Document doc4 = new Document()) {
                            doc4.addText("region", "North");
                            doc4.addText("category", "Books");
                            doc4.addText("quarter", "Q1");
                            doc4.addInteger("sales_amount", 300);
                            doc4.addInteger("price", 25);
                            doc4.addInteger("rating", 70);
                            writer.addDocument(doc4);
                        }

                        // South + Electronics + Q1
                        try (Document doc5 = new Document()) {
                            doc5.addText("region", "South");
                            doc5.addText("category", "Electronics");
                            doc5.addText("quarter", "Q1");
                            doc5.addInteger("sales_amount", 1200);
                            doc5.addInteger("price", 600);
                            doc5.addInteger("rating", 88);
                            writer.addDocument(doc5);
                        }

                        // South + Books + Q2
                        try (Document doc6 = new Document()) {
                            doc6.addText("region", "South");
                            doc6.addText("category", "Books");
                            doc6.addText("quarter", "Q2");
                            doc6.addInteger("sales_amount", 250);
                            doc6.addInteger("price", 20);
                            doc6.addInteger("rating", 75);
                            writer.addDocument(doc6);
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
    public void testMultiTermsAggregationBasic() {
        System.out.println("🚀 Testing basic multi-dimensional GROUP BY...");

        // Create multi-dimensional GROUP BY aggregation
        MultiTermsAggregation multiGroup = new MultiTermsAggregation("sales_analysis",
            new String[]{"region", "category"}, 100, 0);

        SplitQuery matchAllQuery = new SplitMatchAllQuery();

        System.out.println("📊 MultiTerms aggregation: " + multiGroup);
        System.out.println("📊 Generated JSON: " + multiGroup.toAggregationJson());

        // Execute search
        SearchResult result = searcher.search(matchAllQuery, 1000, "multi_agg", multiGroup);

        System.out.println("🔍 hasAggregations(): " + result.hasAggregations());
        assertTrue(result.hasAggregations(), "Should have aggregations");

        // Get the root terms result for region
        TermsResult rootTerms = (TermsResult) result.getAggregation("multi_agg");
        assertNotNull(rootTerms, "Root terms result should not be null");

        System.out.println("📊 Root terms result: " + rootTerms);
        System.out.println("📊 Number of region buckets: " + rootTerms.getBuckets().size());

        // Create MultiTermsResult from nested structure
        MultiTermsResult multiTermsResult = new MultiTermsResult("multi_agg", rootTerms,
            new String[]{"region", "category"});

        System.out.println("📊 MultiTerms result: " + multiTermsResult);
        System.out.println("📊 Number of multi-dimensional buckets: " + multiTermsResult.getBucketCount());

        // Validate buckets
        assertTrue(multiTermsResult.getBucketCount() >= 3,
            "Should have at least 3 region+category combinations");

        // Test multi-dimensional bucket access
        for (MultiTermsResult.MultiTermsBucket bucket : multiTermsResult.getBuckets()) {
            String[] fieldValues = bucket.getFieldValues();
            assertEquals(2, fieldValues.length, "Should have 2 field values (region, category)");

            String region = fieldValues[0];
            String category = fieldValues[1];
            long docCount = bucket.getDocCount();

            System.out.println("📦 Bucket: " + region + "/" + category + " (count=" + docCount + ")");

            // Validate known combinations
            assertNotNull(region, "Region should not be null");
            assertNotNull(category, "Category should not be null");
            assertTrue(docCount > 0, "Doc count should be positive");

            // Validate field value access methods
            assertEquals(region, bucket.getFieldValue(0), "Region should match first field value");
            assertEquals(category, bucket.getFieldValue(1), "Category should match second field value");

            // Test composite key generation
            String compositeKey = bucket.getCompositeKey();
            assertEquals(region + "|" + category, compositeKey, "Composite key should join field values");
        }

        System.out.println("✅ Basic multi-dimensional GROUP BY test completed successfully!");
    }

    @Test
    public void testMultiTermsAggregationWithSubAggregations() {
        System.out.println("🚀 Testing multi-dimensional GROUP BY with sub-aggregations...");

        // Create multi-dimensional GROUP BY with sub-aggregations (the core feature request)
        MultiTermsAggregation multiGroup = new MultiTermsAggregation("business_analysis",
            new String[]{"region", "category", "quarter"}, 100, 0);

        // Add metric sub-aggregations
        multiGroup.addSubAggregation("total_sales", new SumAggregation("sales_amount"));
        multiGroup.addSubAggregation("avg_price", new AverageAggregation("price"));
        multiGroup.addSubAggregation("max_rating", new MaxAggregation("rating"));

        SplitQuery matchAllQuery = new SplitMatchAllQuery();

        System.out.println("📊 MultiTerms with sub-aggs: " + multiGroup);
        System.out.println("📊 Generated JSON: " + multiGroup.toAggregationJson());

        // Execute search
        SearchResult result = searcher.search(matchAllQuery, 1000, "business_agg", multiGroup);

        assertTrue(result.hasAggregations(), "Should have aggregations");

        // Get root terms result and convert to multi-dimensional result
        TermsResult rootTerms = (TermsResult) result.getAggregation("business_agg");
        assertNotNull(rootTerms, "Root terms result should not be null");

        MultiTermsResult multiTermsResult = new MultiTermsResult("business_agg", rootTerms,
            new String[]{"region", "category", "quarter"});

        System.out.println("📊 Multi-dimensional result: " + multiTermsResult);
        System.out.println("📊 Number of 3D buckets: " + multiTermsResult.getBucketCount());

        // Validate 3-dimensional buckets with sub-aggregations
        boolean foundNorthElectronicsQ1 = false;
        for (MultiTermsResult.MultiTermsBucket bucket : multiTermsResult.getBuckets()) {
            String[] fieldValues = bucket.getFieldValues();
            assertEquals(3, fieldValues.length, "Should have 3 field values (region, category, quarter)");

            String region = fieldValues[0];
            String category = fieldValues[1];
            String quarter = fieldValues[2];
            long docCount = bucket.getDocCount();

            System.out.println("📦 3D Bucket: " + region + "/" + category + "/" + quarter + " (count=" + docCount + ")");

            // Validate sub-aggregations exist
            assertTrue(bucket.hasSubAggregations(), "Bucket should have sub-aggregations");

            SumResult totalSales = (SumResult) bucket.getSubAggregation("total_sales");
            AverageResult avgPrice = (AverageResult) bucket.getSubAggregation("avg_price");
            MaxResult maxRating = (MaxResult) bucket.getSubAggregation("max_rating");

            assertNotNull(totalSales, "Total sales sub-aggregation should exist");
            assertNotNull(avgPrice, "Average price sub-aggregation should exist");
            assertNotNull(maxRating, "Max rating sub-aggregation should exist");

            System.out.println("  💰 Total sales: " + totalSales.getSum());
            System.out.println("  💵 Average price: " + avgPrice.getAverage());
            System.out.println("  ⭐ Max rating: " + maxRating.getMax());

            // Validate specific known bucket (case-insensitive since Tantivy may lowercase terms)
            if ("north".equalsIgnoreCase(region) && "electronics".equalsIgnoreCase(category) && "q1".equalsIgnoreCase(quarter)) {
                foundNorthElectronicsQ1 = true;
                assertEquals(2, docCount, "North/Electronics/Q1 should have 2 documents");
                assertEquals(2500.0, totalSales.getSum(), 0.01, "Total sales should be 2500");
                assertEquals(625.0, avgPrice.getAverage(), 0.01, "Average price should be 625");
                assertEquals(90.0, maxRating.getMax(), 0.01, "Max rating should be 90");
            }
        }

        assertTrue(foundNorthElectronicsQ1, "Should find North/Electronics/Q1 bucket");

        System.out.println("✅ Multi-dimensional GROUP BY with sub-aggregations test completed successfully!");
    }

    @Test
    public void testMultiTermsAggregationJSON() {
        System.out.println("🧪 Testing nested aggregation JSON generation...");

        // Test 2-field aggregation JSON
        MultiTermsAggregation twoField = new MultiTermsAggregation("test_2d",
            new String[]{"region", "category"}, 50, 10);

        String json2d = twoField.toAggregationJson();
        System.out.println("📊 2D JSON: " + json2d);

        // Verify structure
        assertTrue(json2d.contains("\"terms\""), "Should contain terms aggregation");
        assertTrue(json2d.contains("\"field\": \"region\""), "Should contain region field");
        assertTrue(json2d.contains("\"size\": 50"), "Should contain size parameter");
        assertTrue(json2d.contains("\"shard_size\": 10"), "Should contain shard_size parameter");
        assertTrue(json2d.contains("\"aggs\""), "Should contain nested aggregations");
        assertTrue(json2d.contains("\"category_terms\""), "Should contain category terms aggregation");

        // Test 3-field aggregation JSON
        MultiTermsAggregation threeField = new MultiTermsAggregation("test_3d",
            new String[]{"year", "month", "day"}, 100, 0);
        threeField.addSubAggregation("count_total", new CountAggregation("id"));

        String json3d = threeField.toAggregationJson();
        System.out.println("📊 3D JSON: " + json3d);

        // Verify nested structure
        assertTrue(json3d.contains("\"field\": \"year\""), "Should contain year field");
        assertTrue(json3d.contains("\"month_terms\""), "Should contain month terms aggregation");
        assertTrue(json3d.contains("\"day_terms\""), "Should contain day terms aggregation");
        assertTrue(json3d.contains("\"count_total\""), "Should contain user sub-aggregation");
        assertTrue(json3d.contains("\"value_count\""), "Should contain count aggregation type");

        System.out.println("✅ Nested aggregation JSON generation verified!");
    }

    @Test
    public void testMultiTermsAggregationValidation() {
        System.out.println("🧪 Testing MultiTermsAggregation validation...");

        // Test validation cases
        assertThrows(IllegalArgumentException.class, () ->
            new MultiTermsAggregation(null), "Should reject null fields");

        assertThrows(IllegalArgumentException.class, () ->
            new MultiTermsAggregation(new String[]{}), "Should reject empty fields");

        assertThrows(IllegalArgumentException.class, () ->
            new MultiTermsAggregation(new String[]{"single"}), "Should reject single field");

        assertThrows(IllegalArgumentException.class, () ->
            new MultiTermsAggregation(new String[]{"field1", null}), "Should reject null field name");

        assertThrows(IllegalArgumentException.class, () ->
            new MultiTermsAggregation(new String[]{"field1", ""}), "Should reject empty field name");

        assertThrows(IllegalArgumentException.class, () ->
            new MultiTermsAggregation("test", new String[]{"field1", "field2"}, 0, 0), "Should reject zero size");

        // Test valid construction
        MultiTermsAggregation valid = new MultiTermsAggregation(new String[]{"field1", "field2"});
        assertNotNull(valid, "Should create valid aggregation");
        assertEquals("field1", valid.getFieldName(), "Should return first field as field name");
        assertArrayEquals(new String[]{"field1", "field2"}, valid.getFields(), "Should return all fields");

        System.out.println("✅ MultiTermsAggregation validation tests passed!");
    }

    @Test
    public void testMultiTermsResultFieldAccess() {
        System.out.println("🧪 Testing MultiTermsResult field access methods...");

        // Create sample bucket
        String[] fieldValues = {"North", "Electronics", "Q1"};
        String[] fieldNames = {"region", "category", "quarter"};

        MultiTermsResult.MultiTermsBucket bucket = new MultiTermsResult.MultiTermsBucket(
            fieldValues, 5, Map.of());

        // Test field access methods
        assertArrayEquals(fieldValues, bucket.getFieldValues(), "Should return all field values");
        assertEquals("North", bucket.getFieldValue(0), "Should return region");
        assertEquals("Electronics", bucket.getFieldValue(1), "Should return category");
        assertEquals("Q1", bucket.getFieldValue(2), "Should return quarter");

        // Test field values map
        Map<String, String> fieldMap = bucket.getFieldValuesMap(fieldNames);
        assertEquals("North", fieldMap.get("region"), "Should map region correctly");
        assertEquals("Electronics", fieldMap.get("category"), "Should map category correctly");
        assertEquals("Q1", fieldMap.get("quarter"), "Should map quarter correctly");

        // Test composite key generation
        assertEquals("North|Electronics|Q1", bucket.getCompositeKey(), "Should generate composite key");
        assertEquals("North/Electronics/Q1", bucket.getCompositeKey("/"), "Should use custom delimiter");

        // Test bounds checking
        assertThrows(IndexOutOfBoundsException.class, () ->
            bucket.getFieldValue(-1), "Should reject negative index");
        assertThrows(IndexOutOfBoundsException.class, () ->
            bucket.getFieldValue(3), "Should reject out of bounds index");

        System.out.println("✅ MultiTermsResult field access tests passed!");
    }
}
