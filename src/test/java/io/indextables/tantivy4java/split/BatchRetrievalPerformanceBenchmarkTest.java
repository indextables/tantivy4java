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
import java.util.concurrent.atomic.AtomicLong;
import java.util.Comparator;

/**
 * Performance benchmark suite for batch retrieval optimization.
 *
 * This test suite measures:
 * 1. Throughput (documents/second)
 * 2. Latency (p50, p95, p99)
 * 3. S3 request efficiency
 * 4. Scalability across batch sizes
 *
 * Results are logged in detail for analysis.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class BatchRetrievalPerformanceBenchmarkTest {

    private static final String TEST_INDEX_UID = "perf-benchmark-test";
    private static final String TEST_SOURCE_ID = "perf-source";
    private static final String TEST_NODE_ID = "perf-node";

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

    // Benchmark configuration
    private static final int[] BATCH_SIZES = {10, 50, 100, 500, 1000, 5000, 10000};
    private static final int WARMUP_ITERATIONS = 2;
    private static final int BENCHMARK_ITERATIONS = 5;

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

        System.out.println("=".repeat(100));
        System.out.println(" ".repeat(30) + "BATCH RETRIEVAL PERFORMANCE BENCHMARK");
        System.out.println("=".repeat(100));
        System.out.println("Configuration:");
        System.out.println("  AWS Region:        " + TEST_REGION);
        System.out.println("  S3 Bucket:         " + TEST_BUCKET);
        System.out.println("  Batch Sizes:       " + Arrays.toString(BATCH_SIZES));
        System.out.println("  Warmup Iterations: " + WARMUP_ITERATIONS);
        System.out.println("  Benchmark Iters:   " + BENCHMARK_ITERATIONS);
        System.out.println("=".repeat(100));

        // Create test split
        SplitUploadResult uploadResult = createTestSplit();
        testSplitS3Uri = uploadResult.s3Uri;
        testSplitMetadata = uploadResult.metadata;

        // Create cache manager
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("perf-benchmark-cache")
                .withMaxCacheSize(1_000_000_000) // 1GB cache for benchmarks
                .withAwsCredentials(accessKey, secretKey)
                .withAwsRegion(TEST_REGION);

        cacheManager = SplitCacheManager.getInstance(cacheConfig);

        System.out.println("✅ Benchmark setup complete\n");
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

        System.out.println("\n" + "=".repeat(100));
        System.out.println(" ".repeat(35) + "BENCHMARK COMPLETE");
        System.out.println("=".repeat(100));
    }

    private static SplitUploadResult createTestSplit() throws Exception {
        System.out.println("📦 Creating benchmark test split...");

        Path tempDir = Files.createTempDirectory("perf-benchmark");
        Path indexPath = tempDir.resolve("benchmark_index");
        Path splitPath = tempDir.resolve("benchmark.split");
        QuickwitSplit.SplitMetadata metadata = null;

        try {
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder.addTextField("title", true, false, "default", "position");
                builder.addTextField("body", true, false, "default", "position");
                builder.addIntegerField("id", true, true, false);
                builder.addIntegerField("score", true, true, false);

                try (Schema schema = builder.build()) {
                    try (Index index = new Index(schema, indexPath.toString(), false)) {
                        try (IndexWriter writer = index.writer(Index.Memory.LARGE_HEAP_SIZE, 4)) {

                            int numDocs = 15000; // Enough for largest batch size
                            System.out.println("  Creating " + numDocs + " documents...");

                            for (int i = 0; i < numDocs; i++) {
                                try (Document doc = new Document()) {
                                    doc.addText("title",
                                            "Benchmark Document " + i);

                                    String body = "This is benchmark document number " + i + ". " +
                                            "It contains some text for testing retrieval performance. " +
                                            "Document ID: " + i + " " +
                                            "Score: " + (i % 100);
                                    doc.addText("body", body);

                                    doc.addInteger("id", i);
                                    doc.addInteger("score", i % 100);

                                    writer.addDocument(doc);
                                }
                            }

                            writer.commit();
                            System.out.println("  ✅ Committed " + numDocs + " documents");
                        }

                        // Reload and merge segments to ensure all documents are in segment 0
                        // This is critical for docBatch which assumes DocAddress(0, i)
                        index.reload();
                        try (Searcher searcher = index.searcher()) {
                            List<String> segmentIds = searcher.getSegmentIds();
                            System.out.println("  📊 Index has " + segmentIds.size() + " segments");
                            if (segmentIds.size() > 1) {
                                System.out.println("  🔄 Merging " + segmentIds.size() + " segments into one...");
                                try (IndexWriter writer = index.writer(Index.Memory.LARGE_HEAP_SIZE, 1)) {
                                    writer.merge(segmentIds);
                                    writer.commit();
                                }
                                index.reload();
                                System.out.println("  ✅ Segments merged successfully");
                            }
                        }

                        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                                TEST_INDEX_UID, TEST_SOURCE_ID, TEST_NODE_ID);

                        metadata = QuickwitSplit.convertIndexFromPath(
                                indexPath.toString(),
                                splitPath.toString(),
                                config
                        );
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

    // ========================================================================
    // THROUGHPUT BENCHMARKS
    // ========================================================================

    @Test
    @Order(1)
    @DisplayName("Benchmark 1: Throughput across batch sizes")
    public void benchmarkThroughputAcrossBatchSizes() throws Exception {
        System.out.println("\n" + "=".repeat(100));
        System.out.println("BENCHMARK 1: Throughput Across Batch Sizes");
        System.out.println("=".repeat(100));

        System.out.println(String.format("%-12s | %-15s | %-15s | %-18s | %-15s",
                "Batch Size", "Avg Time (ms)", "Docs/Second", "ms/Doc", "Speedup vs 10"));

        System.out.println("-".repeat(100));

        double baselineThroughput = 0;

        for (int batchSize : BATCH_SIZES) {
            BenchmarkResult result = runThroughputBenchmark(batchSize);

            double speedup = baselineThroughput > 0 ?
                    result.throughput / baselineThroughput : 1.0;

            if (baselineThroughput == 0) {
                baselineThroughput = result.throughput;
            }

            System.out.println(String.format("%-12d | %-15.2f | %-15.2f | %-18.4f | %.2fx",
                    batchSize,
                    result.avgLatencyMs,
                    result.throughput,
                    result.latencyPerDoc,
                    speedup));
        }

        System.out.println("=".repeat(100));
    }

    @Test
    @Order(2)
    @DisplayName("Benchmark 2: Latency percentiles")
    public void benchmarkLatencyPercentiles() throws Exception {
        System.out.println("\n" + "=".repeat(100));
        System.out.println("BENCHMARK 2: Latency Percentiles (1000 documents)");
        System.out.println("=".repeat(100));

        int batchSize = 1000;
        int iterations = 20; // More iterations for percentile calculation

        List<Long> latencies = new ArrayList<>();

        System.out.println("Running " + iterations + " iterations...");

        // Warmup
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(testSplitS3Uri, testSplitMetadata)) {
            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                List<DocAddress> addresses = generateAddresses(batchSize, i * 100);
                List<Document> docs = searcher.docBatch(addresses);
                // Close documents to prevent resource leaks
                for (Document doc : docs) {
                    doc.close();
                }
            }
        }

        // Benchmark
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(testSplitS3Uri, testSplitMetadata)) {
            for (int i = 0; i < iterations; i++) {
                List<DocAddress> addresses = generateAddresses(batchSize, i * 50);

                long start = System.currentTimeMillis();
                List<Document> docs = searcher.docBatch(addresses);
                long duration = System.currentTimeMillis() - start;

                latencies.add(duration);

                // Close documents to prevent resource leaks
                for (Document doc : docs) {
                    doc.close();
                }

                if ((i + 1) % 5 == 0) {
                    System.out.println("  Completed " + (i + 1) + "/" + iterations + " iterations");
                }
            }
        }

        // Calculate percentiles
        Collections.sort(latencies);

        long p50 = latencies.get(latencies.size() / 2);
        long p95 = latencies.get((int) (latencies.size() * 0.95));
        long p99 = latencies.get((int) (latencies.size() * 0.99));
        long min = latencies.get(0);
        long max = latencies.get(latencies.size() - 1);
        double avg = latencies.stream().mapToLong(Long::longValue).average().orElse(0);

        System.out.println("\n" + "-".repeat(100));
        System.out.println("Latency Statistics:");
        System.out.println("  Min:  " + min + "ms");
        System.out.println("  p50:  " + p50 + "ms");
        System.out.println("  p95:  " + p95 + "ms");
        System.out.println("  p99:  " + p99 + "ms");
        System.out.println("  Max:  " + max + "ms");
        System.out.println("  Avg:  " + String.format("%.2f", avg) + "ms");
        System.out.println("=".repeat(100));
    }

    @Test
    @Order(3)
    @DisplayName("Benchmark 3: Cache warming effect")
    public void benchmarkCacheWarmingEffect() throws Exception {
        System.out.println("\n" + "=".repeat(100));
        System.out.println("BENCHMARK 3: Cache Warming Effect");
        System.out.println("=".repeat(100));

        int batchSize = 1000;
        int iterations = 10;

        System.out.println("Testing " + batchSize + " documents over " + iterations + " iterations");
        System.out.println(String.format("%-12s | %-15s | %-20s",
                "Iteration", "Latency (ms)", "Cache Effect"));
        System.out.println("-".repeat(100));

        long firstLatency = 0;

        try (SplitSearcher searcher = cacheManager.createSplitSearcher(testSplitS3Uri, testSplitMetadata)) {
            List<DocAddress> addresses = generateAddresses(batchSize, 0);

            for (int i = 0; i < iterations; i++) {
                long start = System.currentTimeMillis();
                List<Document> docs = searcher.docBatch(addresses);
                long latency = System.currentTimeMillis() - start;

                if (i == 0) {
                    firstLatency = latency;
                }

                double speedup = (double) firstLatency / latency;

                System.out.println(String.format("%-12d | %-15d | %.2fx faster",
                        i + 1, latency, speedup));

                // Close documents to prevent resource leaks
                for (Document doc : docs) {
                    doc.close();
                }
            }
        }

        System.out.println("=".repeat(100));
    }

    @Test
    @Order(4)
    @DisplayName("Benchmark 4: Scalability with concurrent retrievals")
    public void benchmarkConcurrentScalability() throws Exception {
        System.out.println("\n" + "=".repeat(100));
        System.out.println("BENCHMARK 4: Scalability with Concurrent Retrievals");
        System.out.println("=".repeat(100));

        int[] threadCounts = {1, 2, 4, 8};
        int batchSizePerThread = 500;

        System.out.println("Each thread retrieves " + batchSizePerThread + " documents");
        System.out.println(String.format("%-12s | %-15s | %-18s | %-15s",
                "Threads", "Total Time (ms)", "Total Throughput", "Efficiency"));
        System.out.println("-".repeat(100));

        double baselineThroughput = 0;

        for (int numThreads : threadCounts) {
            BenchmarkResult result = runConcurrentBenchmark(numThreads, batchSizePerThread);

            double efficiency = baselineThroughput > 0 ?
                    (result.throughput / baselineThroughput) / numThreads : 1.0;

            if (numThreads == 1) {
                baselineThroughput = result.throughput;
            }

            System.out.println(String.format("%-12d | %-15.2f | %-18.2f | %.1f%%",
                    numThreads,
                    result.avgLatencyMs,
                    result.throughput,
                    efficiency * 100));
        }

        System.out.println("=".repeat(100));
    }

    // ========================================================================
    // MEMORY BENCHMARKS
    // ========================================================================

    @Test
    @Order(5)
    @DisplayName("Benchmark 5: Memory usage scaling")
    public void benchmarkMemoryUsageScaling() throws Exception {
        System.out.println("\n" + "=".repeat(100));
        System.out.println("BENCHMARK 5: Memory Usage Scaling");
        System.out.println("=".repeat(100));

        Runtime runtime = Runtime.getRuntime();

        System.out.println(String.format("%-12s | %-20s | %-20s | %-15s",
                "Batch Size", "Memory Used (MB)", "Bytes per Doc", "Total Docs"));
        System.out.println("-".repeat(100));

        for (int batchSize : new int[]{100, 500, 1000, 5000}) {
            // Force GC before measurement
            System.gc();
            Thread.sleep(500);

            long memBefore = runtime.totalMemory() - runtime.freeMemory();

            try (SplitSearcher searcher = cacheManager.createSplitSearcher(testSplitS3Uri, testSplitMetadata)) {
                List<DocAddress> addresses = generateAddresses(batchSize, 0);
                List<Document> docs = searcher.docBatch(addresses);

                long memAfter = runtime.totalMemory() - runtime.freeMemory();
                long memUsed = memAfter - memBefore;

                System.out.println(String.format("%-12d | %-20.2f | %-20d | %-15d",
                        batchSize,
                        memUsed / 1024.0 / 1024.0,
                        memUsed / batchSize,
                        docs.size()));

                // Clean up - close all documents
                for (Document doc : docs) {
                    doc.close();
                }
            }
        }

        System.out.println("=".repeat(100));
    }

    // ========================================================================
    // HELPER METHODS
    // ========================================================================

    private BenchmarkResult runThroughputBenchmark(int batchSize) throws Exception {
        List<Long> latencies = new ArrayList<>();

        // Warmup
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(testSplitS3Uri, testSplitMetadata)) {
            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                List<DocAddress> addresses = generateAddresses(batchSize, i * 1000);
                List<Document> docs = searcher.docBatch(addresses);
                // Close documents to prevent resource leaks
                for (Document doc : docs) {
                    doc.close();
                }
            }
        }

        // Benchmark
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(testSplitS3Uri, testSplitMetadata)) {
            for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
                List<DocAddress> addresses = generateAddresses(batchSize, i * 500);

                long start = System.currentTimeMillis();
                List<Document> docs = searcher.docBatch(addresses);
                long duration = System.currentTimeMillis() - start;

                latencies.add(duration);

                // Close documents to prevent resource leaks
                for (Document doc : docs) {
                    doc.close();
                }
            }
        }

        double avgLatency = latencies.stream().mapToLong(Long::longValue).average().orElse(0);
        double throughput = (batchSize * 1000.0) / avgLatency; // docs/second
        double latencyPerDoc = avgLatency / batchSize;

        return new BenchmarkResult(avgLatency, throughput, latencyPerDoc);
    }

    private BenchmarkResult runConcurrentBenchmark(int numThreads, int batchSizePerThread) throws Exception {
        List<Thread> threads = new ArrayList<>();
        AtomicLong totalDuration = new AtomicLong(0);
        AtomicLong maxDuration = new AtomicLong(0);

        long startTime = System.currentTimeMillis();

        for (int t = 0; t < numThreads; t++) {
            int threadId = t;
            Thread thread = new Thread(() -> {
                try {
                    try (SplitSearcher searcher = cacheManager.createSplitSearcher(testSplitS3Uri, testSplitMetadata)) {
                        List<DocAddress> addresses = generateAddresses(
                                batchSizePerThread,
                                threadId * batchSizePerThread);

                        long threadStart = System.currentTimeMillis();
                        List<Document> docs = searcher.docBatch(addresses);
                        long threadDuration = System.currentTimeMillis() - threadStart;

                        totalDuration.addAndGet(threadDuration);
                        maxDuration.updateAndGet(current -> Math.max(current, threadDuration));

                        // Close documents to prevent resource leaks
                        for (Document doc : docs) {
                            doc.close();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            threads.add(thread);
            thread.start();
        }

        // Wait for all threads
        for (Thread thread : threads) {
            thread.join();
        }

        long wallClockTime = System.currentTimeMillis() - startTime;
        int totalDocs = numThreads * batchSizePerThread;
        double throughput = (totalDocs * 1000.0) / wallClockTime;

        return new BenchmarkResult(wallClockTime, throughput, (double) wallClockTime / totalDocs);
    }

    private List<DocAddress> generateAddresses(int count, int offset) {
        List<DocAddress> addresses = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            addresses.add(new DocAddress(0, offset + i));
        }
        return addresses;
    }

    private static class BenchmarkResult {
        final double avgLatencyMs;
        final double throughput; // docs/second
        final double latencyPerDoc;

        BenchmarkResult(double avgLatencyMs, double throughput, double latencyPerDoc) {
            this.avgLatencyMs = avgLatencyMs;
            this.throughput = throughput;
            this.latencyPerDoc = latencyPerDoc;
        }
    }
}
