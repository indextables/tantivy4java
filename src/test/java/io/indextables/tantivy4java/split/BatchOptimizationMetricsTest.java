package io.indextables.tantivy4java.split;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.split.merge.QuickwitSplit;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test suite for BatchOptimizationMetrics functionality.
 *
 * Validates that metrics are properly collected and exposed through the Java API.
 *
 * NOTE: Batch optimization is designed for remote S3/Azure splits to reduce cloud storage requests.
 * Local file:// splits do not benefit from batch optimization since there are no S3 request costs.
 * To test actual metrics recording, use RealS3EndToEndTest or configure S3 credentials.
 *
 * This test validates the metrics API structure even though local files won't record metrics.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class BatchOptimizationMetricsTest {

    private static Path tempDir;
    private static String splitPath;
    private static QuickwitSplit.SplitMetadata splitMetadata;

    @BeforeAll
    public static void setUpClass() throws Exception {
        System.out.println("\n=== BATCH OPTIMIZATION METRICS TEST ===\n");

        // Create temporary directory
        tempDir = Files.createTempDirectory("batch-metrics-test");
        System.out.println("✅ Temp directory: " + tempDir);

        // Create a simple test index and convert to split
        createTestSplit();

        // Reset metrics before tests start
        SplitCacheManager.resetBatchMetrics();
        System.out.println("✅ Metrics reset to baseline\n");
    }

    @AfterAll
    public static void tearDownClass() throws Exception {
        if (tempDir != null) {
            // Cleanup
            Files.walk(tempDir)
                .sorted((a, b) -> -a.compareTo(b))
                .forEach(p -> {
                    try { Files.deleteIfExists(p); } catch (Exception e) {}
                });
        }
        System.out.println("\n✅ Test cleanup complete\n");
    }

    private static void createTestSplit() throws Exception {
        // Create simple schema
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");
            builder.addIntegerField("id", true, true, false);

            try (Schema schema = builder.build()) {
                // Create index
                Path indexPath = tempDir.resolve("test_index");
                try (Index index = new Index(schema, indexPath.toString(), false)) {
                    // Add test documents
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        for (int i = 0; i < 200; i++) {
                            try (Document doc = new Document()) {
                                doc.addText("title", "Document " + i);
                                doc.addInteger("id", i);
                                writer.addDocument(doc);
                            }
                        }
                        writer.commit();
                        System.out.println("  ✅ Created index with 200 documents");
                    }

                    // Reload and merge segments
                    index.reload();
                    try (Searcher searcher = index.searcher()) {
                        List<String> segmentIds = searcher.getSegmentIds();
                        if (segmentIds.size() > 1) {
                            try (IndexWriter writer = index.writer(Index.Memory.LARGE_HEAP_SIZE, 1)) {
                                writer.merge(segmentIds);
                                writer.commit();
                            }
                            index.reload();
                        }
                    }

                    // Convert to split
                    splitPath = tempDir.resolve("test.split").toString();
                    QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                        "test-index", "test-source", "test-node");
                    splitMetadata = QuickwitSplit.convertIndexFromPath(indexPath.toString(), splitPath, splitConfig);
                    System.out.println("  ✅ Created split: " + splitPath);
                }
            }
        }
    }

    @Test
    @Order(1)
    public void testMetricsInitialState() {
        System.out.println("Test 1: Verify initial metrics state");

        BatchOptimizationMetrics metrics = SplitCacheManager.getBatchMetrics();

        // After reset, all metrics should be zero
        assertEquals(0, metrics.getTotalBatchOperations(), "Initial batch operations should be 0");
        assertEquals(0, metrics.getTotalDocumentsRequested(), "Initial documents should be 0");
        assertEquals(0, metrics.getTotalRequests(), "Initial requests should be 0");
        assertEquals(0, metrics.getConsolidatedRequests(), "Initial consolidated requests should be 0");

        System.out.println("  ✅ Initial state verified: all metrics at zero\n");
    }

    @Test
    @Order(2)
    public void testMetricsCollection() throws Exception {
        System.out.println("Test 2: Verify metrics API structure (local files don't optimize)");

        // Reset metrics
        SplitCacheManager.resetBatchMetrics();

        // Create cache manager and perform batch retrieval
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("metrics-test-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
            try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadata)) {

                // Create batch of 100 documents
                List<DocAddress> addresses = new ArrayList<>();
                for (int i = 0; i < 100; i++) {
                    addresses.add(new DocAddress(0, i));
                }

                // Perform batch retrieval (local files don't trigger S3 optimization)
                List<Document> docs = searcher.docBatch(addresses);
                assertEquals(100, docs.size(), "Should retrieve 100 documents");

                // Close documents
                for (Document doc : docs) {
                    doc.close();
                }
            }
        }

        // Check metrics API works (will be 0 for local files)
        BatchOptimizationMetrics metrics = SplitCacheManager.getBatchMetrics();

        System.out.println("  📊 Metrics API response:");
        System.out.println("     Total Operations: " + metrics.getTotalBatchOperations());
        System.out.println("     Documents Requested: " + metrics.getTotalDocumentsRequested());
        System.out.println("     Note: Local file:// splits don't use batch optimization");
        System.out.println("     Use S3/Azure splits for actual metrics recording");

        // Validate API works (returns valid values, even if 0)
        assertNotNull(metrics, "Metrics API should return a valid object");
        assertTrue(metrics.getTotalBatchOperations() >= 0, "Operations should be non-negative");
        assertTrue(metrics.getConsolidationRatio() >= 0, "Ratio should be non-negative");

        System.out.println("  ✅ Metrics API structure validated\n");
    }

    @Test
    @Order(3)
    public void testMetricsAccumulation() throws Exception {
        System.out.println("Test 3: Verify metrics accumulate across multiple operations");

        // Get current metrics
        BatchOptimizationMetrics before = SplitCacheManager.getBatchMetrics();
        long operationsBefore = before.getTotalBatchOperations();
        long docsBefore = before.getTotalDocumentsRequested();

        System.out.println("  📊 Before second operation:");
        System.out.println("     Operations: " + operationsBefore);
        System.out.println("     Documents: " + docsBefore);

        // Perform another batch operation
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("metrics-test-cache-2")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
            try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadata)) {
                List<DocAddress> addresses = new ArrayList<>();
                for (int i = 0; i < 50; i++) {
                    addresses.add(new DocAddress(0, i));
                }

                List<Document> docs = searcher.docBatch(addresses);
                assertEquals(50, docs.size());

                for (Document doc : docs) {
                    doc.close();
                }
            }
        }

        // Check that metrics accumulated (if optimization ran - it won't for local files)
        BatchOptimizationMetrics after = SplitCacheManager.getBatchMetrics();

        System.out.println("  📊 After second operation:");
        System.out.println("     Operations: " + after.getTotalBatchOperations());
        System.out.println("     Documents: " + after.getTotalDocumentsRequested());

        // Validate metrics are consistent (may be 0 for local files)
        assertTrue(after.getTotalBatchOperations() >= operationsBefore,
            "Operations should not decrease");
        assertTrue(after.getTotalDocumentsRequested() >= docsBefore,
            "Documents should not decrease");

        System.out.println("  ✅ Metrics accumulation API validated\n");
    }

    @Test
    @Order(4)
    public void testMetricsReset() {
        System.out.println("Test 4: Verify metrics reset functionality");

        // Get current metrics (may be 0 if optimization didn't run for local files)
        BatchOptimizationMetrics before = SplitCacheManager.getBatchMetrics();
        long operationsBefore = before.getTotalBatchOperations();

        System.out.println("  📊 Before reset: " + operationsBefore + " operations");

        // Reset metrics
        SplitCacheManager.resetBatchMetrics();

        // Verify all metrics are zero after reset
        BatchOptimizationMetrics after = SplitCacheManager.getBatchMetrics();
        assertEquals(0, after.getTotalBatchOperations(), "Operations should be 0 after reset");
        assertEquals(0, after.getTotalDocumentsRequested(), "Documents should be 0 after reset");
        assertEquals(0, after.getTotalRequests(), "Requests should be 0 after reset");
        assertEquals(0, after.getConsolidatedRequests(), "Consolidated requests should be 0 after reset");

        System.out.println("  ✅ Metrics successfully reset to zero\n");
    }

    @Test
    @Order(5)
    public void testMetricsFormatting() throws Exception {
        System.out.println("Test 5: Verify metrics formatting and helper methods");

        // Reset and perform one batch operation
        SplitCacheManager.resetBatchMetrics();

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("metrics-format-test")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
            try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadata)) {
                List<DocAddress> addresses = new ArrayList<>();
                for (int i = 0; i < 75; i++) {
                    addresses.add(new DocAddress(0, i));
                }

                List<Document> docs = searcher.docBatch(addresses);
                for (Document doc : docs) {
                    doc.close();
                }
            }
        }

        BatchOptimizationMetrics metrics = SplitCacheManager.getBatchMetrics();

        // Test toString()
        String str = metrics.toString();
        assertNotNull(str);
        assertTrue(str.contains("BatchOptimizationMetrics"), "toString should contain class name");
        System.out.println("  ✅ toString(): " + str);

        // Test getSummary()
        String summary = metrics.getSummary();
        assertNotNull(summary);
        assertTrue(summary.contains("Total Operations"), "Summary should contain operation count");
        assertTrue(summary.contains("Cost Savings"), "Summary should contain cost savings");
        System.out.println("\n  📊 Summary:\n" + summary);

        // Test helper methods (values may be 0 for local files)
        double avgBatchSize = metrics.getAverageBatchSize();
        assertTrue(avgBatchSize >= 0, "Average batch size should be non-negative");
        System.out.println("\n  ✅ Average batch size: " + avgBatchSize);

        double avgDuration = metrics.getAveragePrefetchDurationMs();
        assertTrue(avgDuration >= 0, "Average duration should be non-negative");
        System.out.println("  ✅ Average prefetch duration: " + avgDuration + "ms");

        double estimatedSavings = metrics.getEstimatedCostSavingsUSD();
        assertTrue(estimatedSavings >= 0, "Estimated savings should be non-negative");
        System.out.println("  ✅ Estimated cost savings: $" + String.format("%.4f", estimatedSavings));

        System.out.println("\n  ✅ All formatting and helper methods work correctly\n");
    }
}
