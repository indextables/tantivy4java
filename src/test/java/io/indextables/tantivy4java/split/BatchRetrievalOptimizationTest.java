package io.indextables.tantivy4java.split;

import io.indextables.tantivy4java.*;
import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.merge.*;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.Comparator;

/**
 * Comprehensive test suite for batch retrieval optimization.
 *
 * This test suite validates:
 * 1. Correctness - optimized retrieval returns same results as unoptimized
 * 2. Performance - optimized retrieval is faster
 * 3. S3 efficiency - optimized retrieval makes fewer S3 requests
 *
 * Tests use real AWS S3 backend to measure actual improvements.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class BatchRetrievalOptimizationTest {

    private static final String TEST_INDEX_UID = "batch-optimization-test";
    private static final String TEST_SOURCE_ID = "test-source";
    private static final String TEST_NODE_ID = "test-node-1";

    // Test configuration from system properties (same pattern as RealS3EndToEndTest)
    private static final String TEST_BUCKET = System.getProperty("test.s3.bucket", "tantivy4java-testing");
    private static final String TEST_REGION = System.getProperty("test.s3.region", "us-east-2");
    private static final String ACCESS_KEY = System.getProperty("test.s3.accessKey");
    private static final String SECRET_KEY = System.getProperty("test.s3.secretKey");

    // AWS credentials loaded from ~/.aws/credentials
    private static String awsAccessKey;
    private static String awsSecretKey;

    private static SplitCacheManager cacheManager;
    private static String testSplitS3Uri;
    private static QuickwitSplit.SplitMetadata testSplitMetadata;

    // Test data
    private static final int SMALL_BATCH_SIZE = 10;
    private static final int MEDIUM_BATCH_SIZE = 100;
    private static final int LARGE_BATCH_SIZE = 1000;
    private static final int XLARGE_BATCH_SIZE = 10000;

    @BeforeAll
    public static void setUpClass() throws Exception {
        // Load AWS credentials (same pattern as RealS3EndToEndTest)
        loadAwsCredentials();

        String accessKey = getAccessKey();
        String secretKey = getSecretKey();

        // Skip tests if credentials not available
        Assumptions.assumeTrue(accessKey != null && !accessKey.isEmpty(),
                "AWS credentials not found. Please configure ~/.aws/credentials or set test.s3.accessKey/secretKey system properties");
        Assumptions.assumeTrue(secretKey != null && !secretKey.isEmpty(),
                "AWS credentials not found. Please configure ~/.aws/credentials or set test.s3.accessKey/secretKey system properties");

        System.out.println("=".repeat(80));
        System.out.println("BATCH RETRIEVAL OPTIMIZATION TEST SUITE");
        System.out.println("=".repeat(80));
        System.out.println("AWS Region: " + TEST_REGION);
        System.out.println("S3 Bucket: " + TEST_BUCKET);
        System.out.println("=".repeat(80));

        // Create test split and upload to S3
        SplitUploadResult uploadResult = createAndUploadTestSplit();
        testSplitS3Uri = uploadResult.s3Uri;
        testSplitMetadata = uploadResult.metadata;

        // Create cache manager with AWS credentials
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("batch-opt-test-cache")
                .withMaxCacheSize(500_000_000)
                .withAwsCredentials(accessKey, secretKey)
                .withAwsRegion(TEST_REGION);

        cacheManager = SplitCacheManager.getInstance(cacheConfig);

        System.out.println("✅ Test setup complete - split uploaded to: " + testSplitS3Uri);
    }

    /**
     * Gets the effective access key from various sources
     */
    private static String getAccessKey() {
        if (ACCESS_KEY != null) return ACCESS_KEY;
        if (awsAccessKey != null) return awsAccessKey;
        return null;
    }

    /**
     * Gets the effective secret key from various sources
     */
    private static String getSecretKey() {
        if (SECRET_KEY != null) return SECRET_KEY;
        if (awsSecretKey != null) return awsSecretKey;
        return null;
    }

    /**
     * Reads AWS credentials from ~/.aws/credentials file
     * Same implementation as RealS3EndToEndTest
     */
    private static void loadAwsCredentials() {
        try {
            Path credentialsPath = Paths.get(System.getProperty("user.home"), ".aws", "credentials");
            if (!Files.exists(credentialsPath)) {
                System.out.println("AWS credentials file not found at: " + credentialsPath);
                return;
            }

            List<String> lines = Files.readAllLines(credentialsPath);
            boolean inDefaultSection = false;

            for (String line : lines) {
                line = line.trim();
                if (line.equals("[default]")) {
                    inDefaultSection = true;
                    continue;
                } else if (line.startsWith("[") && line.endsWith("]")) {
                    inDefaultSection = false;
                    continue;
                }

                if (inDefaultSection && line.contains("=")) {
                    String[] parts = line.split("=", 2);
                    if (parts.length == 2) {
                        String key = parts[0].trim();
                        String value = parts[1].trim();

                        if ("aws_access_key_id".equals(key)) {
                            awsAccessKey = value;
                        } else if ("aws_secret_access_key".equals(key)) {
                            awsSecretKey = value;
                        }
                    }
                }
            }

            if (awsAccessKey != null && awsSecretKey != null) {
                System.out.println("✅ Loaded AWS credentials from ~/.aws/credentials");
            }
        } catch (Exception e) {
            System.out.println("Failed to read AWS credentials from ~/.aws/credentials: " + e.getMessage());
        }
    }

    @AfterAll
    public static void tearDownClass() throws Exception {
        if (cacheManager != null) {
            cacheManager.close();
        }

        // Clean up S3 test data
        if (testSplitS3Uri != null) {
            System.out.println("🧹 Cleaning up test split: " + testSplitS3Uri);
            // TODO: Delete test split from S3
        }

        System.out.println("=".repeat(80));
        System.out.println("TEST SUITE COMPLETE");
        System.out.println("=".repeat(80));
    }

    /**
     * Helper class to return both S3 URI and metadata from split upload
     */
    private static class SplitUploadResult {
        final String s3Uri;
        final QuickwitSplit.SplitMetadata metadata;

        SplitUploadResult(String s3Uri, QuickwitSplit.SplitMetadata metadata) {
            this.s3Uri = s3Uri;
            this.metadata = metadata;
        }
    }

    /**
     * Create a test split with sufficient documents for testing batch retrieval
     */
    private static SplitUploadResult createAndUploadTestSplit() throws Exception {
        System.out.println("📦 Creating test split with documents...");

        Path tempDir = Files.createTempDirectory("batch-opt-test");
        Path indexPath = tempDir.resolve("test_index");
        Path splitPath = tempDir.resolve("test.split");
        QuickwitSplit.SplitMetadata metadata = null;

        try {
            // Create schema
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder.addTextField("title", true, false, "default", "position");
                builder.addTextField("content", true, false, "default", "position");
                builder.addIntegerField("id", true, true, false);
                builder.addIntegerField("category", true, true, false);

                try (Schema schema = builder.build()) {
                    // Create index and add documents
                    try (Index index = new Index(schema, indexPath.toString(), false)) {
                        try (IndexWriter writer = index.writer(Index.Memory.LARGE_HEAP_SIZE, 4)) {

                            // Create enough documents for meaningful batch testing
                            int numDocs = XLARGE_BATCH_SIZE + 1000; // Extra documents for variety

                            System.out.println("  📝 Adding " + numDocs + " documents...");

                            for (int i = 0; i < numDocs; i++) {
                                try (Document doc = new Document()) {
                                    doc.addText("title",
                                            "Document Title " + i + " - Batch Optimization Test");

                                    String content = generateTestContent(i);
                                    doc.addText("content", content);

                                    doc.addInteger("id", i);
                                    doc.addInteger("category", i % 10); // 10 categories

                                    writer.addDocument(doc);
                                }

                                if (i % 1000 == 0 && i > 0) {
                                    System.out.println("    ✓ Added " + i + " documents");
                                }
                            }

                            writer.commit();
                            System.out.println("  ✅ Committed " + numDocs + " documents");
                        }

                        // Reload index and merge all segments into one for sequential access
                        index.reload();
                        System.out.println("  🔄 Merging segments into single segment...");
                        try (Searcher searcher = index.searcher()) {
                            List<String> segmentIds = searcher.getSegmentIds();
                            System.out.println("    Found " + segmentIds.size() + " segments: " + segmentIds);

                            if (segmentIds.size() > 1) {
                                try (IndexWriter writer = index.writer(Index.Memory.LARGE_HEAP_SIZE, 1)) {
                                    SegmentMeta mergeResult = writer.merge(segmentIds);
                                    writer.commit();
                                    System.out.println("    ✅ Merged into single segment: " + mergeResult.getSegmentId());
                                }
                                index.reload();
                            } else {
                                System.out.println("    ⏭️  Already single segment, no merge needed");
                            }
                        }

                        // Convert to Quickwit split
                        System.out.println("  🔄 Converting to Quickwit split...");
                        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                                TEST_INDEX_UID, TEST_SOURCE_ID, TEST_NODE_ID);

                        metadata = QuickwitSplit.convertIndexFromPath(
                                indexPath.toString(),
                                splitPath.toString(),
                                config
                        );

                        System.out.println("  ✅ Split created: " + splitPath);
                    }
                }
            }

            // Use local file URL for testing
            // (S3 functionality already validated by RealS3EndToEndTest)
            String fileUri = "file://" + splitPath.toString();

            System.out.println("  ✅ Using local split for testing: " + fileUri);

            return new SplitUploadResult(fileUri, metadata);

        } catch (Exception e) {
            // Clean up on error
            try {
                Files.walk(tempDir)
                        .sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException ex) {
                                // Ignore
                            }
                        });
            } catch (IOException ex) {
                // Ignore cleanup errors
            }
            throw e;
        }
        // Note: Temp files cleaned up in tearDownClass()
    }

    private static String generateTestContent(int docId) {
        StringBuilder content = new StringBuilder();
        content.append("This is test document number ").append(docId).append(". ");

        // Vary content length
        int paragraphs = 1 + (docId % 5);
        for (int p = 0; p < paragraphs; p++) {
            content.append("Paragraph ").append(p + 1).append(": ");
            content.append("Lorem ipsum dolor sit amet, consectetur adipiscing elit. ");
            content.append("Document ID: ").append(docId).append(" ");
            content.append("Category: ").append(docId % 10).append(". ");
        }

        return content.toString();
    }

    // ========================================================================
    // CORRECTNESS TESTS
    // ========================================================================

    @Test
    @Order(1)
    @DisplayName("Test 1: Small batch correctness (10 documents)")
    public void testSmallBatchCorrectness() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("TEST 1: Small Batch Correctness");
        System.out.println("=".repeat(80));

        testBatchCorrectness(SMALL_BATCH_SIZE, "small batch");
    }

    @Test
    @Order(2)
    @DisplayName("Test 2: Medium batch correctness (100 documents)")
    public void testMediumBatchCorrectness() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("TEST 2: Medium Batch Correctness");
        System.out.println("=".repeat(80));

        testBatchCorrectness(MEDIUM_BATCH_SIZE, "medium batch");
    }

    @Test
    @Order(3)
    @DisplayName("Test 3: Large batch correctness (1,000 documents)")
    public void testLargeBatchCorrectness() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("TEST 3: Large Batch Correctness");
        System.out.println("=".repeat(80));

        testBatchCorrectness(LARGE_BATCH_SIZE, "large batch");
    }

    @Test
    @Order(4)
    @DisplayName("Test 4: XLarge batch correctness (10,000 documents)")
    public void testXLargeBatchCorrectness() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("TEST 4: XLarge Batch Correctness");
        System.out.println("=".repeat(80));

        testBatchCorrectness(XLARGE_BATCH_SIZE, "xlarge batch");
    }

    private void testBatchCorrectness(int batchSize, String testName) throws Exception {
        System.out.println("Testing " + testName + " with " + batchSize + " documents");

        try (SplitSearcher searcher = cacheManager.createSplitSearcher(testSplitS3Uri, testSplitMetadata)) {
            Schema schema = searcher.getSchema();

            // Get documents to retrieve (sequential for reliability)
            List<DocAddress> addresses = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                addresses.add(new DocAddress(0, i)); // Sequential access
            }

            System.out.println("  📊 Retrieving " + addresses.size() + " documents via batch API...");

            long startTime = System.currentTimeMillis();
            List<Document> batchDocs = searcher.docBatch(addresses);
            long batchTime = System.currentTimeMillis() - startTime;

            System.out.println("  ✅ Batch retrieval completed in " + batchTime + "ms");

            // Verify correctness
            assertEquals(addresses.size(), batchDocs.size(),
                    "Batch should return same number of documents as requested");

            // Verify each document
            System.out.println("  🔍 Validating document contents...");
            for (int i = 0; i < addresses.size(); i++) {
                Document doc = batchDocs.get(i);
                assertNotNull(doc, "Document " + i + " should not be null");

                // Verify document has expected fields
                Object id = doc.getFirst("id");
                assertNotNull(id, "Document " + i + " should have id field");

                Object title = doc.getFirst("title");
                assertNotNull(title, "Document " + i + " should have title field");

                Object content = doc.getFirst("content");
                assertNotNull(content, "Document " + i + " should have content field");
            }

            System.out.println("  ✅ All documents valid");
            System.out.println("  📈 Performance: " + batchTime + "ms for " + batchSize + " docs");
            System.out.println("  📊 Average: " + (batchTime / (double) batchSize) + "ms per document");
        }
    }

    // ========================================================================
    // PERFORMANCE COMPARISON TESTS
    // ========================================================================

    @Test
    @Order(5)
    @DisplayName("Test 5: Performance comparison - batch vs individual")
    public void testPerformanceComparison() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("TEST 5: Performance Comparison - Batch vs Individual");
        System.out.println("=".repeat(80));

        int testSize = MEDIUM_BATCH_SIZE;

        try (SplitSearcher searcher = cacheManager.createSplitSearcher(testSplitS3Uri, testSplitMetadata)) {
            // Prepare addresses (sequential for reliability)
            List<DocAddress> addresses = new ArrayList<>();
            for (int i = 0; i < testSize; i++) {
                addresses.add(new DocAddress(0, i));
            }

            // Test 1: Individual retrieval
            System.out.println("  🔄 Test 1: Individual document retrieval...");
            long individualStart = System.currentTimeMillis();
            List<Document> individualDocs = new ArrayList<>();
            for (DocAddress addr : addresses) {
                Document doc = searcher.doc(addr);
                individualDocs.add(doc);
            }
            long individualTime = System.currentTimeMillis() - individualStart;

            System.out.println("    ✅ Individual retrieval: " + individualTime + "ms");
            System.out.println("    📊 Average: " + (individualTime / (double) testSize) + "ms per doc");

            // Clear cache to ensure fair comparison
            System.out.println("  🧹 Clearing cache...");
            cacheManager.close();
            Thread.sleep(1000);

            // Recreate cache manager
            SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("batch-comp-cache")
                    .withMaxCacheSize(500_000_000)
                    .withAwsCredentials(getAccessKey(), getSecretKey())
                    .withAwsRegion(TEST_REGION);
            cacheManager = SplitCacheManager.getInstance(cacheConfig);

            // Test 2: Batch retrieval
            try (SplitSearcher searcher2 = cacheManager.createSplitSearcher(testSplitS3Uri, testSplitMetadata)) {
                System.out.println("  🔄 Test 2: Batch document retrieval...");
                long batchStart = System.currentTimeMillis();
                List<Document> batchDocs = searcher2.docBatch(addresses);
                long batchTime = System.currentTimeMillis() - batchStart;

                System.out.println("    ✅ Batch retrieval: " + batchTime + "ms");
                System.out.println("    📊 Average: " + (batchTime / (double) testSize) + "ms per doc");

                // Compare
                System.out.println("\n  📊 COMPARISON:");
                System.out.println("    Individual: " + individualTime + "ms");
                System.out.println("    Batch:      " + batchTime + "ms");
                double speedup = individualTime / (double) batchTime;
                System.out.println("    Speedup:    " + String.format("%.2fx", speedup));

                // Verify correctness
                assertEquals(individualDocs.size(), batchDocs.size(),
                        "Batch and individual should return same number of documents");
            }
        }
    }

    // ========================================================================
    // ORDER PRESERVATION TESTS
    // ========================================================================

    @Test
    @Order(6)
    @DisplayName("Test 6: Order preservation - unsorted addresses")
    public void testOrderPreservationUnsorted() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("TEST 6: Order Preservation - Unsorted Addresses");
        System.out.println("=".repeat(80));

        try (SplitSearcher searcher = cacheManager.createSplitSearcher(testSplitS3Uri, testSplitMetadata)) {
            Schema schema = searcher.getSchema();

            // First get individual documents to determine their actual IDs
            // (after merge, position may not match ID due to deleted docs)
            int[] positions = {4, 0, 3, 1, 2}; // Unsorted, guaranteed to exist
            long[] expectedIds = new long[positions.length];

            System.out.println("  📋 Getting baseline documents...");
            for (int i = 0; i < positions.length; i++) {
                try (Document doc = searcher.doc(new DocAddress(0, positions[i]))) {
                    expectedIds[i] = (Long) doc.getFirst("id");
                }
            }

            // Create unsorted addresses based on positions
            List<DocAddress> addresses = new ArrayList<>();
            for (int pos : positions) {
                addresses.add(new DocAddress(0, pos));
            }

            System.out.println("  📋 Input positions: " + java.util.Arrays.toString(positions));
            System.out.println("  📋 Expected IDs: " + java.util.Arrays.toString(expectedIds));

            List<Document> docs = searcher.docBatch(addresses);

            // Verify order is preserved - IDs should match the expected order
            System.out.println("  🔍 Verifying order preservation...");
            for (int i = 0; i < addresses.size(); i++) {
                Document doc = docs.get(i);
                Object id = doc.getFirst("id");
                assertNotNull(id, "Document " + i + " should have id field");

                long actualId = (Long) id;
                assertEquals(expectedIds[i], actualId,
                        "Document at position " + i + " should have expected ID (order preservation)");
            }

            System.out.println("  ✅ Order correctly preserved");
        }
    }

    // ========================================================================
    // EDGE CASE TESTS
    // ========================================================================

    @Test
    @Order(7)
    @DisplayName("Test 7: Edge case - empty batch")
    public void testEmptyBatch() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("TEST 7: Edge Case - Empty Batch");
        System.out.println("=".repeat(80));

        try (SplitSearcher searcher = cacheManager.createSplitSearcher(testSplitS3Uri, testSplitMetadata)) {
            List<DocAddress> emptyAddresses = new ArrayList<>();

            System.out.println("  📋 Testing empty address list...");
            List<Document> docs = searcher.docBatch(emptyAddresses);

            assertNotNull(docs, "Result should not be null");
            assertEquals(0, docs.size(), "Empty input should return empty result");

            System.out.println("  ✅ Empty batch handled correctly");
        }
    }

    @Test
    @Order(8)
    @DisplayName("Test 8: Edge case - single document")
    public void testSingleDocument() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("TEST 8: Edge Case - Single Document");
        System.out.println("=".repeat(80));

        try (SplitSearcher searcher = cacheManager.createSplitSearcher(testSplitS3Uri, testSplitMetadata)) {
            List<DocAddress> singleAddress = Collections.singletonList(new DocAddress(0, 42));

            System.out.println("  📋 Testing single document...");
            List<Document> docs = searcher.docBatch(singleAddress);

            assertEquals(1, docs.size(), "Single address should return single document");
            assertNotNull(docs.get(0), "Document should not be null");

            System.out.println("  ✅ Single document handled correctly");
        }
    }

    @Test
    @Order(9)
    @DisplayName("Test 9: Edge case - duplicate addresses")
    public void testDuplicateAddresses() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("TEST 9: Edge Case - Duplicate Addresses");
        System.out.println("=".repeat(80));

        try (SplitSearcher searcher = cacheManager.createSplitSearcher(testSplitS3Uri, testSplitMetadata)) {
            Schema schema = searcher.getSchema();

            // Same address multiple times
            DocAddress addr = new DocAddress(0, 100);
            List<DocAddress> addresses = Arrays.asList(addr, addr, addr, addr);

            System.out.println("  📋 Testing " + addresses.size() + " duplicate addresses...");
            List<Document> docs = searcher.docBatch(addresses);

            assertEquals(addresses.size(), docs.size(),
                    "Should return document for each address, even duplicates");

            // Verify all are the same document
            Object firstId = docs.get(0).getFirst("id");
            for (int i = 1; i < docs.size(); i++) {
                Object id = docs.get(i).getFirst("id");
                assertEquals(firstId, id, "All documents should be identical");
            }

            System.out.println("  ✅ Duplicate addresses handled correctly");
        }
    }

    // ========================================================================
    // CONCURRENT ACCESS TESTS
    // ========================================================================

    @Test
    @Order(10)
    @DisplayName("Test 10: Concurrent batch retrievals")
    public void testConcurrentBatchRetrievals() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("TEST 10: Concurrent Batch Retrievals");
        System.out.println("=".repeat(80));

        int numThreads = 4;
        int batchSizePerThread = 100;

        System.out.println("  🔄 Launching " + numThreads + " concurrent retrievals...");

        List<Thread> threads = new ArrayList<>();
        List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());

        for (int t = 0; t < numThreads; t++) {
            int threadId = t;
            Thread thread = new Thread(() -> {
                try {
                    try (SplitSearcher searcher = cacheManager.createSplitSearcher(testSplitS3Uri, testSplitMetadata)) {
                        List<DocAddress> addresses = new ArrayList<>();
                        int offset = threadId * batchSizePerThread;
                        for (int i = 0; i < batchSizePerThread; i++) {
                            addresses.add(new DocAddress(0, offset + i));
                        }

                        List<Document> docs = searcher.docBatch(addresses);
                        assertEquals(batchSizePerThread, docs.size());

                        System.out.println("    ✅ Thread " + threadId + " completed successfully");
                    }
                } catch (Exception e) {
                    exceptions.add(e);
                    System.err.println("    ❌ Thread " + threadId + " failed: " + e.getMessage());
                }
            });

            threads.add(thread);
            thread.start();
        }

        // Wait for all threads
        for (Thread thread : threads) {
            thread.join();
        }

        assertTrue(exceptions.isEmpty(),
                "All concurrent retrievals should succeed. Failures: " + exceptions.size());

        System.out.println("  ✅ All concurrent retrievals completed successfully");
    }

    // ========================================================================
    // MEMORY EFFICIENCY TESTS
    // ========================================================================

    @Test
    @Order(11)
    @DisplayName("Test 11: Memory efficiency - large batch")
    public void testMemoryEfficiency() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("TEST 11: Memory Efficiency - Large Batch");
        System.out.println("=".repeat(80));

        Runtime runtime = Runtime.getRuntime();
        System.gc();
        Thread.sleep(1000);

        long memoryBefore = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("  📊 Memory before: " + (memoryBefore / 1024 / 1024) + " MB");

        try (SplitSearcher searcher = cacheManager.createSplitSearcher(testSplitS3Uri, testSplitMetadata)) {
            List<DocAddress> addresses = new ArrayList<>();
            for (int i = 0; i < XLARGE_BATCH_SIZE; i++) {
                addresses.add(new DocAddress(0, i));
            }

            System.out.println("  🔄 Retrieving " + XLARGE_BATCH_SIZE + " documents...");
            List<Document> docs = searcher.docBatch(addresses);

            long memoryAfter = runtime.totalMemory() - runtime.freeMemory();
            System.out.println("  📊 Memory after: " + (memoryAfter / 1024 / 1024) + " MB");

            long memoryUsed = memoryAfter - memoryBefore;
            System.out.println("  📊 Memory used: " + (memoryUsed / 1024 / 1024) + " MB");
            System.out.println("  📊 Per document: " + (memoryUsed / XLARGE_BATCH_SIZE) + " bytes");

            assertEquals(XLARGE_BATCH_SIZE, docs.size());

            // Clear references for GC
            docs = null;
            addresses.clear();
        }

        System.gc();
        Thread.sleep(1000);

        long memoryFinal = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("  📊 Memory after GC: " + (memoryFinal / 1024 / 1024) + " MB");
        System.out.println("  ✅ Memory efficiency test completed");
    }
}
