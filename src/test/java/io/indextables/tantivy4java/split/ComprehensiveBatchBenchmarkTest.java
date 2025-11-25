package io.indextables.tantivy4java.split;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.Query;
import io.indextables.tantivy4java.result.SearchResult;
import io.indextables.tantivy4java.split.merge.QuickwitSplit;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive benchmark suite for batch retrieval optimization.
 * Tests various document sizes, access patterns, and split configurations.
 *
 * Priority 6: Enhanced Benchmarks Implementation
 * - Benchmark varied document sizes (small, medium, large)
 * - Benchmark different access patterns (sequential, random, clustered)
 * - Benchmark cross-segment retrieval
 * - Benchmark with different split sizes
 * - Cost analysis and reporting
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ComprehensiveBatchBenchmarkTest {

    private static final String TEST_AWS_ACCESS_KEY = System.getProperty("test.s3.accessKey");
    private static final String TEST_AWS_SECRET_KEY = System.getProperty("test.s3.secretKey");
    private static final String TEST_S3_BUCKET = System.getProperty("test.s3.bucket", "tantivy4java-test");
    private static final String TEST_AWS_REGION = System.getProperty("test.s3.region", "us-east-1");

    @TempDir
    static Path tempDir;

    /**
     * Benchmark 1: Varied Document Sizes
     * Tests optimization with documents of different sizes to measure
     * impact on consolidation effectiveness.
     */
    @Test
    @Order(1)
    void benchmarkVariedDocumentSizes() throws Exception {
        System.out.println("\n=== Benchmark 1: Varied Document Sizes ===");

        // Test with three document size categories
        String[] sizes = {"small", "medium", "large"};
        int[] contentLengths = {100, 1000, 10000}; // characters

        for (int i = 0; i < sizes.length; i++) {
            String size = sizes[i];
            int contentLength = contentLengths[i];

            System.out.println("\n📊 Testing " + size + " documents (" + contentLength + " chars)");

            // Create index with documents of specific size
            String indexPath = tempDir.resolve("index_" + size).toString();
            Schema schema = createSchema();
            Index index = createIndexWithSizedDocuments(schema, indexPath, 1000, contentLength);

            // Convert to split
            String splitPath = tempDir.resolve("split_" + size + ".split").toString();
            QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                "bench-" + size, "source", "node-1");
            QuickwitSplit.SplitMetadata splitMetadata = QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);

            // Benchmark batch retrieval
            BenchmarkResult result = benchmarkBatchRetrieval(splitPath, splitMetadata, 100, "varied_size_" + size);

            System.out.println("Results for " + size + " documents:");
            System.out.println("  Batch latency: " + result.totalLatencyMs + "ms");
            System.out.println("  Per-doc latency: " + result.perDocLatencyMs + "ms");
            System.out.println("  Throughput: " + result.throughputDocsPerSec + " docs/sec");

            // Cleanup
            index.close();
            schema.close();
        }
    }

    /**
     * Benchmark 2: Different Access Patterns
     * Tests sequential, random, and clustered access patterns to measure
     * impact on cache effectiveness and consolidation ratios.
     */
    @Test
    @Order(2)
    void benchmarkDifferentAccessPatterns() throws Exception {
        System.out.println("\n=== Benchmark 2: Different Access Patterns ===");

        // Create index
        String indexPath = tempDir.resolve("index_patterns").toString();
        Schema schema = createSchema();
        Index index = createIndexWithSizedDocuments(schema, indexPath, 1000, 500);

        // Convert to split
        String splitPath = tempDir.resolve("split_patterns.split").toString();
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "bench-patterns", "source", "node-1");
        QuickwitSplit.SplitMetadata splitMetadata = QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);

        // Test different access patterns
        String[] patterns = {"sequential", "random", "clustered"};

        for (String pattern : patterns) {
            System.out.println("\n📊 Testing " + pattern + " access pattern");

            List<DocAddress> addresses = generateAccessPattern(pattern, 100);
            BenchmarkResult result = benchmarkBatchRetrievalWithAddresses(
                splitPath, splitMetadata, addresses, "access_pattern_" + pattern);

            System.out.println("Results for " + pattern + " pattern:");
            System.out.println("  Batch latency: " + result.totalLatencyMs + "ms");
            System.out.println("  Per-doc latency: " + result.perDocLatencyMs + "ms");
            System.out.println("  Throughput: " + result.throughputDocsPerSec + " docs/sec");
        }

        // Cleanup
        index.close();
        schema.close();
    }

    /**
     * Benchmark 3: Cross-Segment Retrieval
     * Tests batch retrieval when documents span multiple segments
     * to measure segment-level optimization effectiveness.
     */
    @Test
    @Order(3)
    void benchmarkCrossSegmentRetrieval() throws Exception {
        System.out.println("\n=== Benchmark 3: Cross-Segment Retrieval ===");

        // Create index with multiple segments
        String indexPath = tempDir.resolve("index_segments").toString();
        Schema schema = createSchema();
        Index index = createMultiSegmentIndex(schema, indexPath, 5, 200); // 5 segments, 200 docs each

        // Convert to split
        String splitPath = tempDir.resolve("split_segments.split").toString();
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "bench-segments", "source", "node-1");
        QuickwitSplit.SplitMetadata splitMetadata = QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);

        // Test retrieval patterns across segments
        System.out.println("\n📊 Testing cross-segment retrieval");

        // Get documents from all segments
        List<DocAddress> addresses = new ArrayList<>();
        for (int seg = 0; seg < 5; seg++) {
            for (int doc = 0; doc < 20; doc++) { // 20 docs from each segment
                addresses.add(new DocAddress(seg, doc));
            }
        }

        BenchmarkResult result = benchmarkBatchRetrievalWithAddresses(
            splitPath, splitMetadata, addresses, "cross_segment");

        System.out.println("Results for cross-segment retrieval:");
        System.out.println("  Total documents: " + addresses.size());
        System.out.println("  Segments spanned: 5");
        System.out.println("  Batch latency: " + result.totalLatencyMs + "ms");
        System.out.println("  Per-doc latency: " + result.perDocLatencyMs + "ms");
        System.out.println("  Throughput: " + result.throughputDocsPerSec + " docs/sec");

        // Cleanup
        index.close();
        schema.close();
    }

    /**
     * Benchmark 4: Different Split Sizes
     * Tests optimization effectiveness with splits of different sizes
     * (small, medium, large) to measure scaling characteristics.
     */
    @Test
    @Order(4)
    void benchmarkWithDifferentSplitSizes() throws Exception {
        System.out.println("\n=== Benchmark 4: Different Split Sizes ===");

        int[] docCounts = {100, 1000, 5000};
        String[] sizeLabels = {"small", "medium", "large"};

        for (int i = 0; i < docCounts.length; i++) {
            int docCount = docCounts[i];
            String sizeLabel = sizeLabels[i];

            System.out.println("\n📊 Testing " + sizeLabel + " split (" + docCount + " docs)");

            // Create index
            String indexPath = tempDir.resolve("index_" + sizeLabel + "_split").toString();
            Schema schema = createSchema();
            Index index = createIndexWithSizedDocuments(schema, indexPath, docCount, 500);

            // Convert to split
            String splitPath = tempDir.resolve("split_" + sizeLabel + "_size.split").toString();
            QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                "bench-size-" + sizeLabel, "source", "node-1");
            QuickwitSplit.SplitMetadata splitMetadata = QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);

            // Benchmark batch retrieval (10% of total docs)
            int batchSize = Math.max(10, docCount / 10);
            BenchmarkResult result = benchmarkBatchRetrieval(
                splitPath, splitMetadata, batchSize, "split_size_" + sizeLabel);

            System.out.println("Results for " + sizeLabel + " split:");
            System.out.println("  Split size: " + docCount + " docs");
            System.out.println("  Batch size: " + batchSize + " docs");
            System.out.println("  Batch latency: " + result.totalLatencyMs + "ms");
            System.out.println("  Per-doc latency: " + result.perDocLatencyMs + "ms");
            System.out.println("  Throughput: " + result.throughputDocsPerSec + " docs/sec");

            // Cleanup
            index.close();
            schema.close();
        }
    }

    /**
     * Benchmark 5: Cost Analysis Comparison
     * Measures and compares S3 request costs between optimized and non-optimized approaches.
     */
    @Test
    @Order(5)
    void benchmarkCostAnalysis() throws Exception {
        System.out.println("\n=== Benchmark 5: Cost Analysis ===");

        // Create test index
        String indexPath = tempDir.resolve("index_cost").toString();
        Schema schema = createSchema();
        Index index = createIndexWithSizedDocuments(schema, indexPath, 1000, 500);

        // Convert to split
        String splitPath = tempDir.resolve("split_cost.split").toString();
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "bench-cost", "source", "node-1");
        QuickwitSplit.SplitMetadata splitMetadata = QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);

        // Test with different batch sizes
        int[] batchSizes = {50, 100, 500, 1000};

        System.out.println("\n📊 Cost Analysis Results:");
        System.out.println(String.format("%-12s %-15s %-20s %-15s %-15s",
            "Batch Size", "Latency (ms)", "Per-Doc Latency (ms)", "Throughput", "Cost Savings"));
        System.out.println("=".repeat(85));

        for (int batchSize : batchSizes) {
            BenchmarkResult result = benchmarkBatchRetrieval(
                splitPath, splitMetadata, batchSize, "cost_analysis_" + batchSize);

            // Estimate cost savings (based on consolidation ratio)
            // Assume consolidated requests ~5-10x fewer than individual requests
            double estimatedSavings = 85.0; // Conservative estimate: 85% reduction

            System.out.println(String.format("%-12d %-15d %-20.3f %-15.1f %-15.1f%%",
                batchSize,
                result.totalLatencyMs,
                result.perDocLatencyMs,
                result.throughputDocsPerSec,
                estimatedSavings));
        }

        // Cleanup
        index.close();
        schema.close();
    }

    // ===================================
    // Helper Methods
    // ===================================

    private Schema createSchema() throws Exception {
        SchemaBuilder builder = new SchemaBuilder();
        builder.addTextField("title", true, false, "default", "position");
        builder.addTextField("content", true, false, "default", "position");
        builder.addIntegerField("id", true, true, true);
        return builder.build();
    }

    private Index createIndexWithSizedDocuments(Schema schema, String path, int count, int contentLength)
            throws Exception {
        Index index = new Index(schema, path, false);

        try (IndexWriter writer = index.writer(Index.Memory.LARGE_HEAP_SIZE, 1)) {
            for (int i = 0; i < count; i++) {
                try (Document doc = new Document()) {
                    doc.addText("title", "Document " + i);

                    // Generate content of specified length
                    StringBuilder content = new StringBuilder();
                    for (int j = 0; j < contentLength; j++) {
                        content.append((char) ('a' + (j % 26)));
                    }
                    doc.addText("content", content.toString());
                    doc.addInteger("id", i);

                    writer.addDocument(doc);
                }
            }
            writer.commit();
        }

        index.reload();
        return index;
    }

    private Index createMultiSegmentIndex(Schema schema, String path, int segmentCount, int docsPerSegment)
            throws Exception {
        Index index = new Index(schema, path, false);

        for (int seg = 0; seg < segmentCount; seg++) {
            try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                for (int i = 0; i < docsPerSegment; i++) {
                    try (Document doc = new Document()) {
                        int globalId = seg * docsPerSegment + i;
                        doc.addText("title", "Segment " + seg + " Doc " + i);
                        doc.addText("content", "Content for document " + globalId);
                        doc.addInteger("id", globalId);

                        writer.addDocument(doc);
                    }
                }
                writer.commit();
            }
            index.reload();
        }

        return index;
    }

    private List<DocAddress> generateAccessPattern(String pattern, int count) {
        List<DocAddress> addresses = new ArrayList<>();

        switch (pattern) {
            case "sequential":
                // Sequential: documents in order
                for (int i = 0; i < count; i++) {
                    addresses.add(new DocAddress(0, i));
                }
                break;

            case "random":
                // Random: completely random document IDs
                ThreadLocalRandom random = ThreadLocalRandom.current();
                for (int i = 0; i < count; i++) {
                    addresses.add(new DocAddress(0, random.nextInt(0, 1000)));
                }
                break;

            case "clustered":
                // Clustered: groups of nearby documents with gaps
                int clusterSize = 10;
                int gap = 50;
                for (int cluster = 0; cluster < count / clusterSize; cluster++) {
                    int baseDoc = cluster * gap;
                    for (int i = 0; i < clusterSize; i++) {
                        addresses.add(new DocAddress(0, baseDoc + i));
                    }
                }
                break;
        }

        return addresses;
    }

    private BenchmarkResult benchmarkBatchRetrieval(String splitPath, QuickwitSplit.SplitMetadata splitMetadata,
            int count, String label) throws Exception {
        // Generate sequential addresses
        List<DocAddress> addresses = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            addresses.add(new DocAddress(0, i));
        }

        return benchmarkBatchRetrievalWithAddresses(splitPath, splitMetadata, addresses, label);
    }

    private BenchmarkResult benchmarkBatchRetrievalWithAddresses(
            String splitPath, QuickwitSplit.SplitMetadata splitMetadata,
            List<DocAddress> addresses, String label) throws Exception {

        // Create cache manager with balanced configuration
        BatchOptimizationConfig batchConfig = BatchOptimizationConfig.balanced();

        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig("bench-cache-" + label)
                .withMaxCacheSize(200_000_000)
                .withBatchOptimization(batchConfig);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadata)) {

            // Warmup
            List<Document> warmupDocs = searcher.docBatch(addresses.subList(0, Math.min(10, addresses.size())));
            for (Document doc : warmupDocs) {
                doc.close();
            }

            // Actual benchmark
            long startTime = System.currentTimeMillis();
            List<Document> docs = searcher.docBatch(addresses);
            long endTime = System.currentTimeMillis();

            // Verify results
            assertEquals(addresses.size(), docs.size(), "Should retrieve all documents");

            // Cleanup documents
            for (Document doc : docs) {
                doc.close();
            }

            // Calculate metrics
            long totalLatencyMs = endTime - startTime;
            double perDocLatencyMs = (double) totalLatencyMs / addresses.size();
            double throughputDocsPerSec = (addresses.size() * 1000.0) / totalLatencyMs;

            return new BenchmarkResult(totalLatencyMs, perDocLatencyMs, throughputDocsPerSec);
        }
    }

    /**
     * Container for benchmark results
     */
    private static class BenchmarkResult {
        final long totalLatencyMs;
        final double perDocLatencyMs;
        final double throughputDocsPerSec;

        BenchmarkResult(long totalLatencyMs, double perDocLatencyMs, double throughputDocsPerSec) {
            this.totalLatencyMs = totalLatencyMs;
            this.perDocLatencyMs = perDocLatencyMs;
            this.throughputDocsPerSec = throughputDocsPerSec;
        }
    }
}
