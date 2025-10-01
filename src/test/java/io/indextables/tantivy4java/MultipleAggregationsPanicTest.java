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
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to reproduce the multiple aggregations panic reported from Scala integration.
 * This replicates the exact scenario that causes "leaf search panicked" errors.
 */
public class MultipleAggregationsPanicTest {

    private SplitSearcher searcher;
    private SplitCacheManager cacheManager;
    private QuickwitSplit.SplitMetadata metadata;
    private String splitPath;

    @BeforeEach
    public void setUp(@TempDir Path tempDir) throws Exception {
        // Create schema matching the failing Scala test exactly
        String indexPath = tempDir.resolve("test_index").toString();
        splitPath = tempDir.resolve("test.split").toString(); // Use "test" like Scala

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder
                .addTextField("content", true, false, "default", "position")
                .addIntegerField("score", true, true, true)     // fast field enabled
                .addIntegerField("value", true, true, true);    // fast field enabled

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        // Add test documents matching Scala pattern
                        for (int i = 1; i <= 10; i++) {
                            try (Document doc = new Document()) {
                                doc.addText("content", "content for document " + i);
                                doc.addInteger("score", i);
                                doc.addInteger("value", i + 9); // (i + 9) pattern from Scala
                                writer.addDocument(doc);
                            }
                        }

                        writer.commit();
                    }

                    // Convert to QuickwitSplit using "test" as split name
                    QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                        "test", "test-source", "test-node"); // Use "test" split ID like Scala

                    metadata = QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);
                    System.out.println("✅ Split created: " + metadata.getSplitId() +
                        " with " + metadata.getNumDocs() + " docs");
                }
            }
        }

        // Create cache manager with unique cache name
        String cacheName = "panic-test-cache-" + System.nanoTime();
        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig(cacheName)
                .withMaxCacheSize(200_000_000);

        cacheManager = SplitCacheManager.getInstance(cacheConfig);
        searcher = cacheManager.createSplitSearcher("file://" + splitPath, metadata);
    }

    @AfterEach
    public void tearDown() {
        if (searcher != null) {
            searcher.close();
            searcher = null;
        }
        cacheManager = null;
        metadata = null;
        splitPath = null;

        // Force cleanup
        System.gc();
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    public void testIndividualAggregationsWork() {
        System.out.println("🧪 Testing individual aggregations (should work)...");

        SplitQuery query = new SplitMatchAllQuery();

        // Individual aggregations should work (as reported)
        CountAggregation countAgg = new CountAggregation("value"); // Use "value" field like Scala
        SearchResult countResult = searcher.aggregate(query, "count", countAgg);
        CountResult count = (CountResult) countResult.getAggregation("count");
        assertNotNull(count, "Count result should not be null");
        assertEquals(10, count.getCount(), "Should have 10 documents");
        System.out.println("✅ COUNT: " + count);

        SumAggregation sumAgg = new SumAggregation("score");
        SearchResult sumResult = searcher.aggregate(query, "sum", sumAgg);
        SumResult sum = (SumResult) sumResult.getAggregation("sum");
        assertNotNull(sum, "Sum result should not be null");
        assertEquals(55.0, sum.getSum(), 0.01, "Sum should be 1+2+...+10 = 55");
        System.out.println("✅ SUM: " + sum);
    }

    @Test
    public void testMultipleAggregationsPanic() {
        System.out.println("🧪 Testing multiple aggregations (reproducing panic)...");

        // Create the exact same aggregations pattern as the failing Scala test
        Map<String, SplitAggregation> aggregations = new HashMap<>();
        aggregations.put("doc_count", new CountAggregation("value"));  // Count using "value" field
        aggregations.put("score_sum", new SumAggregation("score"));    // Sum using "score" field

        SplitQuery query = new SplitMatchAllQuery();

        System.out.println("📊 Aggregation map: " + aggregations);
        System.out.println("🔍 Split ID: " + metadata.getSplitId());
        System.out.println("📄 Document count: " + metadata.getNumDocs());

        // This should reproduce the "leaf search panicked" error
        try {
            SearchResult result = searcher.search(query, 10, aggregations);

            // If we get here, the panic didn't occur
            assertTrue(result.hasAggregations(), "Result should have aggregations");

            CountResult docCount = (CountResult) result.getAggregation("doc_count");
            SumResult scoreSum = (SumResult) result.getAggregation("score_sum");

            assertNotNull(docCount, "Doc count should not be null");
            assertNotNull(scoreSum, "Score sum should not be null");

            assertEquals(10, docCount.getCount(), "Should count 10 documents");
            assertEquals(55.0, scoreSum.getSum(), 0.01, "Sum should be 55");

            System.out.println("✅ Multiple aggregations succeeded!");
            System.out.println("   Doc count: " + docCount);
            System.out.println("   Score sum: " + scoreSum);

        } catch (RuntimeException e) {
            if (e.getMessage().contains("leaf search panicked")) {
                System.out.println("❌ REPRODUCED: leaf search panicked error!");
                System.out.println("   Error: " + e.getMessage());
                // This is the bug we're trying to fix - fail the test to confirm reproduction
                fail("Multiple aggregations caused panic: " + e.getMessage());
            } else {
                // Some other error - re-throw
                throw e;
            }
        }
    }

    @Test
    public void testMultipleAggregationsDifferentCombinations() {
        System.out.println("🧪 Testing different multiple aggregation combinations...");

        SplitQuery query = new SplitMatchAllQuery();

        // Try different combinations to see which ones cause panics
        Map<String, Map<String, SplitAggregation>> testCases = new HashMap<>();

        // Case 1: Count + Average (different from the failing case)
        Map<String, SplitAggregation> case1 = new HashMap<>();
        case1.put("doc_count", new CountAggregation("value"));
        case1.put("score_avg", new AverageAggregation("score"));
        testCases.put("COUNT + AVG", case1);

        // Case 2: Sum + Min (different combination)
        Map<String, SplitAggregation> case2 = new HashMap<>();
        case2.put("score_sum", new SumAggregation("score"));
        case2.put("score_min", new MinAggregation("score"));
        testCases.put("SUM + MIN", case2);

        // Case 3: Three aggregations
        Map<String, SplitAggregation> case3 = new HashMap<>();
        case3.put("doc_count", new CountAggregation("value"));
        case3.put("score_sum", new SumAggregation("score"));
        case3.put("score_max", new MaxAggregation("score"));
        testCases.put("COUNT + SUM + MAX", case3);

        for (Map.Entry<String, Map<String, SplitAggregation>> testCase : testCases.entrySet()) {
            String caseName = testCase.getKey();
            Map<String, SplitAggregation> aggregations = testCase.getValue();

            System.out.println("🔬 Testing case: " + caseName);

            try {
                SearchResult result = searcher.search(query, 10, aggregations);
                System.out.println("   ✅ " + caseName + " succeeded");

            } catch (RuntimeException e) {
                if (e.getMessage().contains("leaf search panicked")) {
                    System.out.println("   ❌ " + caseName + " caused panic: " + e.getMessage());
                } else {
                    System.out.println("   ⚠️  " + caseName + " caused other error: " + e.getMessage());
                }
            }
        }
    }
}
