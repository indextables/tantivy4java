package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Comprehensive test for parallel split merge operations with high document counts.
 *
 * Test Scenario:
 * 1. Create a single split with 100,000 documents
 * 2. Make 5 file system copies of the split
 * 3. Merge each set of splits in 5 parallel threads
 * 4. Verify each merged split contains 500,000 documents
 * 5. Verify 5 separate merged splits are created successfully
 */
public class ParallelSplitMergeStressTest {

    @TempDir
    Path tempDir;

    private static final int DOCUMENTS_PER_SPLIT = 80_000;  // Reduced for faster test
    private static final int NUM_COPIES = 5;
    private static final int EXPECTED_MERGED_DOCS = DOCUMENTS_PER_SPLIT * NUM_COPIES; // 50,000
    private static final int NUM_PARALLEL_THREADS = 5;


    @BeforeEach
    void setUp() {
        // Enable debug logging for detailed merge analysis
        System.setProperty("TANTIVY4JAVA_DEBUG", "1");
    }

    @AfterEach
    void tearDown() {
        System.clearProperty("TANTIVY4JAVA_DEBUG");
    }

    @Test
    void testProgressiveParallelSplitMergeScaling() throws Exception {
        System.out.println("🚀 PROGRESSIVE PARALLEL SPLIT MERGE SCALING TEST");
        System.out.println("   Testing 1, 2, 3, 4, 5 parallel merge operations with timing analysis");

        // Step 1: Create base split with documents
        System.out.println("📝 Step 1: Creating base split with " + DOCUMENTS_PER_SPLIT + " documents");
        Path baseSplitPath = createBaseSplitWith100kDocuments();

        // Step 2: Create local split copies for all test scenarios
        System.out.println("📂 Step 2: Creating local split copies for progressive testing");
        Map<Integer, List<List<String>>> allSplitSets = createAllLocalSplitSetsForProgressive(baseSplitPath);

        // Step 3: Progressive testing from 1 to 5 parallel operations
        List<ProgressiveResult> progressiveResults = new ArrayList<>();

        for (int parallelCount = 1; parallelCount <= 5; parallelCount++) {
            System.out.println("\n" + "=".repeat(80));
            System.out.println("⚡ TESTING " + parallelCount + " PARALLEL MERGE OPERATIONS");
            System.out.println("=".repeat(80));

            List<List<String>> splitSets = allSplitSets.get(parallelCount);
            ProgressiveResult result = executeProgressiveTest(parallelCount, splitSets);
            progressiveResults.add(result);

            System.out.println("✅ " + parallelCount + " parallel operations completed in " + result.totalTime + "ms");
            System.out.println("📊 Average per operation: " + result.averageTime + "ms");
            System.out.println("📈 Efficiency: " + String.format("%.1f%%", result.efficiency));
        }

        // Step 4: Analyze scaling performance
        System.out.println("\n" + "=".repeat(80));
        System.out.println("📊 PROGRESSIVE SCALING ANALYSIS");
        System.out.println("=".repeat(80));
        analyzeProgressiveResults(progressiveResults);

        System.out.println("🎉 PROGRESSIVE PARALLEL SCALING TEST: COMPLETED SUCCESSFULLY");
    }

    /**
     * Creates a base split with 100,000 documents using efficient bulk indexing.
     */
    private Path createBaseSplitWith100kDocuments() throws Exception {
        Path indexDir = tempDir.resolve("base_index");
        Files.createDirectories(indexDir);

        // Create schema optimized for bulk operations
        SchemaBuilder builder = new SchemaBuilder();
        Schema schema = builder
            .addTextField("title", true, false, "default", "position")
            .addTextField("content", true, false, "default", "position")
            .addIntegerField("doc_id", true, true, false)
            .addIntegerField("category", true, true, false)
            .build();

        // Create index with large heap for bulk operations
        Index index = new Index(schema, indexDir.toString(), false);

        try (IndexWriter writer = index.writer(Index.Memory.XL_HEAP_SIZE, 4)) {
            System.out.println("📝 Bulk indexing " + DOCUMENTS_PER_SPLIT + " documents...");

            // Bulk index documents in batches for performance
            int batchSize = 1_000;
            for (int batch = 0; batch < DOCUMENTS_PER_SPLIT / batchSize; batch++) {
                List<String> jsonDocs = new ArrayList<>();

                for (int i = 0; i < batchSize; i++) {
                    int docId = batch * batchSize + i;
                    String json = String.format(
                        "{\"title\": \"Document Title %d\", " +
                        "\"content\": \"This is the content of document %d with some searchable text and keywords batch_%d\", " +
                        "\"doc_id\": %d, " +
                        "\"category\": %d}",
                        docId, docId, batch, docId, docId % 10
                    );
                    jsonDocs.add(json);
                }

                // Add batch to writer
                for (String json : jsonDocs) {
                    writer.addJson(json);
                }

                if ((batch + 1) % 5 == 0) {
                    System.out.println("  📊 Indexed " + ((batch + 1) * batchSize) + " documents");
                }
            }

            writer.commit();
            System.out.println("✅ Successfully indexed " + DOCUMENTS_PER_SPLIT + " documents");
        }

        // Convert to split
        Path baseSplitPath = tempDir.resolve("base_split.split");
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "stress-test-index", "bulk-source", "test-node");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
            indexDir.toString(), baseSplitPath.toString(), config);

        System.out.println("📦 Created base split: " + baseSplitPath);
        System.out.println("📊 Split metadata: " + metadata.getNumDocs() + " docs, " + metadata.getUncompressedSizeBytes() + " bytes");

        // Verify document count
        assertEquals(DOCUMENTS_PER_SPLIT, metadata.getNumDocs(),
            "Base split should contain exactly " + DOCUMENTS_PER_SPLIT + " documents");

        return baseSplitPath;
    }


    /**
     * Creates local file copies for parallel merge testing.
     */
    private List<List<String>> createLocalSplitCopies(Path baseSplitPath) throws IOException {
        List<List<String>> splitCopySets = new ArrayList<>();

        for (int threadId = 0; threadId < NUM_PARALLEL_THREADS; threadId++) {
            List<String> splitSet = new ArrayList<>();

            for (int copyId = 0; copyId < NUM_COPIES; copyId++) {
                Path copyPath = tempDir.resolve(String.format("thread_%d_copy_%d.split", threadId, copyId));
                Files.copy(baseSplitPath, copyPath, StandardCopyOption.REPLACE_EXISTING);
                splitSet.add(copyPath.toString());

                System.out.println("📋 Created local copy: " + copyPath.getFileName());
            }

            splitCopySets.add(splitSet);
            System.out.println("📂 Thread " + threadId + " local split set: " + splitSet.size() + " copies");
        }

        return splitCopySets;
    }

    /**
     * Executes parallel merge operations using a thread pool.
     */
    private List<Future<MergeResult>> executeParallelMerges(List<List<String>> splitCopySets) {
        ExecutorService executor = Executors.newFixedThreadPool(NUM_PARALLEL_THREADS);
        List<Future<MergeResult>> futures = new ArrayList<>();
        AtomicInteger threadCounter = new AtomicInteger(0);

        for (List<String> splitSet : splitCopySets) {
            int threadId = threadCounter.getAndIncrement();

            Future<MergeResult> future = executor.submit(() -> {
                try {
                    return performSingleMerge(threadId, splitSet);
                } catch (Exception e) {
                    throw new RuntimeException("Merge failed in thread " + threadId, e);
                }
            });

            futures.add(future);
        }

        executor.shutdown();
        return futures;
    }

    /**
     * Performs a single merge operation for one thread.
     */
    private MergeResult performSingleMerge(int threadId, List<String> splitPaths) throws Exception {
        long startTime = System.currentTimeMillis();
        System.out.println("🧵 Thread " + threadId + ": Starting merge of " + splitPaths.size() + " splits");

        // Configure merge with thread-specific settings (50MB heap)
        long heapSize = 50L * 1024 * 1024; // 50MB in bytes
        QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
            "stress-test-merged-" + threadId,
            "parallel-merge-source",
            "merge-node-" + threadId,
            heapSize);

        // Create local output path
        Path localOutputPath = tempDir.resolve("merged_thread_" + threadId + ".split");
        String outputPath = localOutputPath.toString();

        // Perform merge operation using isolated Rust process
        MergeBinaryExtractor.MergeResult processResult = MergeBinaryExtractor.executeMerge(
            splitPaths, outputPath, mergeConfig);
        QuickwitSplit.SplitMetadata result = processResult.toSplitMetadata();

        long duration = System.currentTimeMillis() - startTime;

        System.out.println("✅ Thread " + threadId + ": Merge completed in " + duration + "ms");
        System.out.println("📊 Thread " + threadId + ": Result - " + result.getNumDocs() + " docs, " + result.getUncompressedSizeBytes() + " bytes");

        return new MergeResult(threadId, localOutputPath, result, duration, splitPaths.size());
    }

    /**
     * Verifies all merge results meet the expected criteria.
     */
    private void verifyMergeResults(List<Future<MergeResult>> futures) throws Exception {
        List<MergeResult> results = new ArrayList<>();

        // Collect all results
        for (int i = 0; i < futures.size(); i++) {
            try {
                MergeResult result = futures.get(i).get(600, TimeUnit.SECONDS); // 2 minute timeout
                results.add(result);
                System.out.println("📋 Thread " + result.threadId + " result collected");
            } catch (TimeoutException e) {
                fail("Merge operation timed out for thread " + i);
            } catch (ExecutionException e) {
                fail("Merge operation failed for thread " + i + ": " + e.getCause().getMessage());
            }
        }

        // Verify we have exactly 5 results
        assertEquals(NUM_PARALLEL_THREADS, results.size(),
            "Should have exactly " + NUM_PARALLEL_THREADS + " merge results");

        // Verify each result
        for (MergeResult result : results) {
            // Check document count
            assertEquals(EXPECTED_MERGED_DOCS, result.metadata.getNumDocs(),
                "Thread " + result.threadId + " merged split should contain " + EXPECTED_MERGED_DOCS + " documents");

            // Check split file exists
            assertTrue(Files.exists(result.outputPath),
                "Thread " + result.threadId + " merged split file should exist: " + result.outputPath);

            // Check file size is reasonable (should be > 0)
            assertTrue(Files.size(result.outputPath) > 0,
                "Thread " + result.threadId + " merged split file should not be empty");

            // Check merge processed correct number of input splits
            assertEquals(NUM_COPIES, result.inputSplitCount,
                "Thread " + result.threadId + " should have merged " + NUM_COPIES + " input splits");

            System.out.println("✅ Thread " + result.threadId + " verification PASSED: " +
                result.metadata.getNumDocs() + " docs, " +
                Files.size(result.outputPath) + " bytes, " +
                result.duration + "ms");
        }

        // Validate each merged split by running queries
        System.out.println("🔍 Starting split validation with search queries...");
        validateMergedSplitsWithQueries(results);

        // Performance analysis
        long totalDuration = results.stream().mapToLong(r -> r.duration).sum();
        long avgDuration = totalDuration / results.size();
        long maxDuration = results.stream().mapToLong(r -> r.duration).max().orElse(0);
        long minDuration = results.stream().mapToLong(r -> r.duration).min().orElse(0);

        System.out.println("📊 PERFORMANCE ANALYSIS:");
        System.out.println("  📈 Average merge time: " + avgDuration + "ms");
        System.out.println("  📉 Min merge time: " + minDuration + "ms");
        System.out.println("  📈 Max merge time: " + maxDuration + "ms");
        System.out.println("  📊 Total parallel time: " + maxDuration + "ms (vs " + totalDuration + "ms sequential)");
        System.out.println("  🚀 Parallelism efficiency: " + String.format("%.1f%%", (double)totalDuration / maxDuration / NUM_PARALLEL_THREADS * 100));

        // Verify parallelism efficiency (should be reasonable)
        double efficiency = (double)totalDuration / maxDuration / NUM_PARALLEL_THREADS * 100;
        assertTrue(efficiency > 50, "Parallelism efficiency should be > 50%, got " + String.format("%.1f%%", efficiency));

        System.out.println("🎉 ALL VERIFICATIONS PASSED!");
    }

    /**
     * Result container for merge operations.
     */
    private static class MergeResult {
        final int threadId;
        final Path outputPath;
        final QuickwitSplit.SplitMetadata metadata;
        final long duration;
        final int inputSplitCount;

        MergeResult(int threadId, Path outputPath, QuickwitSplit.SplitMetadata metadata,
                   long duration, int inputSplitCount) {
            this.threadId = threadId;
            this.outputPath = outputPath;
            this.metadata = metadata;
            this.duration = duration;
            this.inputSplitCount = inputSplitCount;
        }
    }

    /**
     * Result container for progressive testing.
     */
    private static class ProgressiveResult {
        final int parallelCount;
        final long totalTime;
        final long averageTime;
        final long minTime;
        final long maxTime;
        final double efficiency;
        final List<MergeResult> mergeResults;

        ProgressiveResult(int parallelCount, long totalTime, long averageTime, long minTime,
                         long maxTime, double efficiency, List<MergeResult> mergeResults) {
            this.parallelCount = parallelCount;
            this.totalTime = totalTime;
            this.averageTime = averageTime;
            this.minTime = minTime;
            this.maxTime = maxTime;
            this.efficiency = efficiency;
            this.mergeResults = mergeResults;
        }
    }

    // ======================== PROGRESSIVE TESTING METHODS ========================

    /**
     * Creates local split sets for all progressive test scenarios (1-5 parallel operations).
     */
    private Map<Integer, List<List<String>>> createAllLocalSplitSetsForProgressive(Path baseSplitPath) throws IOException {
        Map<Integer, List<List<String>>> allSplitSets = new HashMap<>();

        for (int parallelCount = 1; parallelCount <= 5; parallelCount++) {
            System.out.println("📂 Creating local split sets for " + parallelCount + " parallel operations");
            List<List<String>> splitSets = createLocalSplitCopiesForCount(baseSplitPath, parallelCount);
            allSplitSets.put(parallelCount, splitSets);
        }

        return allSplitSets;
    }

    /**
     * Creates local split copies for a specific parallel count.
     */
    private List<List<String>> createLocalSplitCopiesForCount(Path baseSplitPath, int parallelCount) throws IOException {
        List<List<String>> splitCopySets = new ArrayList<>();

        for (int threadId = 0; threadId < parallelCount; threadId++) {
            List<String> splitSet = new ArrayList<>();

            for (int copyId = 0; copyId < NUM_COPIES; copyId++) {
                Path copyPath = tempDir.resolve(String.format("p%d_thread_%d_copy_%d.split", parallelCount, threadId, copyId));
                Files.copy(baseSplitPath, copyPath, StandardCopyOption.REPLACE_EXISTING);
                splitSet.add(copyPath.toString());
            }

            splitCopySets.add(splitSet);
        }

        return splitCopySets;
    }

    /**
     * Executes a progressive test for a specific parallel count.
     */
    private ProgressiveResult executeProgressiveTest(int parallelCount, List<List<String>> splitSets) throws Exception {
        long testStartTime = System.currentTimeMillis();

        // Execute parallel merges
        ExecutorService executor = Executors.newFixedThreadPool(parallelCount);
        List<Future<MergeResult>> futures = new ArrayList<>();
        AtomicInteger threadCounter = new AtomicInteger(0);

        for (List<String> splitSet : splitSets) {
            int threadId = threadCounter.getAndIncrement();

            Future<MergeResult> future = executor.submit(() -> {
                try {
                    return performProgressiveMerge(parallelCount, threadId, splitSet);
                } catch (Exception e) {
                    throw new RuntimeException("Progressive merge failed in thread " + threadId, e);
                }
            });

            futures.add(future);
        }

        executor.shutdown();

        // Collect results
        List<MergeResult> results = new ArrayList<>();
        for (int i = 0; i < futures.size(); i++) {
            try {
                MergeResult result = futures.get(i).get(600, TimeUnit.SECONDS);
                results.add(result);
            } catch (TimeoutException e) {
                fail("Progressive merge operation timed out for " + parallelCount + " parallel, thread " + i);
            } catch (ExecutionException e) {
                fail("Progressive merge operation failed for " + parallelCount + " parallel, thread " + i + ": " + e.getCause().getMessage());
            }
        }

        long testEndTime = System.currentTimeMillis();
        long totalTime = testEndTime - testStartTime;

        // Calculate statistics
        long sumDurations = results.stream().mapToLong(r -> r.duration).sum();
        long averageTime = sumDurations / results.size();
        long minTime = results.stream().mapToLong(r -> r.duration).min().orElse(0);
        long maxTime = results.stream().mapToLong(r -> r.duration).max().orElse(0);
        double efficiency = (double) sumDurations / totalTime / parallelCount * 100;

        // Verify all results are correct
        for (MergeResult result : results) {
            assertEquals(EXPECTED_MERGED_DOCS, result.metadata.getNumDocs(),
                "Progressive test " + parallelCount + " parallel: Thread " + result.threadId + " should have " + EXPECTED_MERGED_DOCS + " docs");
        }

        return new ProgressiveResult(parallelCount, totalTime, averageTime, minTime, maxTime, efficiency, results);
    }

    /**
     * Performs a single merge operation for progressive testing.
     */
    private MergeResult performProgressiveMerge(int parallelCount, int threadId, List<String> splitPaths) throws Exception {
        long startTime = System.currentTimeMillis();
        System.out.println("🧵 P" + parallelCount + " Thread " + threadId + ": Starting merge of " + splitPaths.size() + " splits");

        // Configure merge with thread-specific settings (50MB heap)
        long heapSize = 50L * 1024 * 1024; // 50MB in bytes
        QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
            "progressive-test-p" + parallelCount + "-t" + threadId,
            "progressive-merge-source",
            "merge-node-p" + parallelCount + "-t" + threadId,
            heapSize);

        // Create local output path
        Path localOutputPath = tempDir.resolve("p" + parallelCount + "_merged_thread_" + threadId + ".split");
        String outputPath = localOutputPath.toString();

        // Perform merge operation using isolated Rust process
        MergeBinaryExtractor.MergeResult processResult = MergeBinaryExtractor.executeMerge(
            splitPaths, outputPath, mergeConfig);
        QuickwitSplit.SplitMetadata result = processResult.toSplitMetadata();

        long duration = System.currentTimeMillis() - startTime;

        System.out.println("✅ P" + parallelCount + " Thread " + threadId + ": Merge completed in " + duration + "ms");

        return new MergeResult(threadId, localOutputPath, result, duration, splitPaths.size());
    }

    /**
     * Analyzes and reports progressive scaling results.
     */
    private void analyzeProgressiveResults(List<ProgressiveResult> results) {
        System.out.println("📊 SCALING PERFORMANCE ANALYSIS:");
        System.out.println();

        // Print detailed results table
        System.out.printf("%-12s %-12s %-12s %-12s %-12s %-12s%n",
            "Parallel", "Total Time", "Avg Time", "Min Time", "Max Time", "Efficiency");
        System.out.println("-".repeat(72));

        for (ProgressiveResult result : results) {
            System.out.printf("%-12d %-12d %-12d %-12d %-12d %-11.1f%%%n",
                result.parallelCount,
                result.totalTime,
                result.averageTime,
                result.minTime,
                result.maxTime,
                result.efficiency);
        }

        System.out.println();

        // Calculate scaling metrics
        ProgressiveResult baseline = results.get(0); // 1 parallel operation
        System.out.println("🚀 SCALING ANALYSIS (vs 1 parallel operation):");
        System.out.println();

        for (int i = 1; i < results.size(); i++) {
            ProgressiveResult current = results.get(i);
            double speedup = (double) baseline.totalTime / current.totalTime;
            double idealSpeedup = current.parallelCount;
            double scalingEfficiency = (speedup / idealSpeedup) * 100;

            System.out.printf("📈 %d parallel operations:%n", current.parallelCount);
            System.out.printf("   Speedup: %.2fx (ideal: %.1fx)%n", speedup, idealSpeedup);
            System.out.printf("   Scaling efficiency: %.1f%%%n", scalingEfficiency);
            System.out.printf("   Time reduction: %d%% (from %dms to %dms)%n",
                (int)((1 - (double)current.totalTime / baseline.totalTime) * 100),
                baseline.totalTime, current.totalTime);
            System.out.println();
        }

        // Find optimal parallel count
        ProgressiveResult best = results.stream()
            .min(Comparator.comparing(r -> r.totalTime))
            .orElse(results.get(results.size() - 1));

        System.out.println("🏆 OPTIMAL CONFIGURATION:");
        System.out.printf("   Best performance: %d parallel operations%n", best.parallelCount);
        System.out.printf("   Total time: %dms%n", best.totalTime);
        System.out.printf("   Efficiency: %.1f%%%n", best.efficiency);

        // Check for diminishing returns
        for (int i = 1; i < results.size(); i++) {
            ProgressiveResult prev = results.get(i - 1);
            ProgressiveResult current = results.get(i);

            if (current.totalTime > prev.totalTime) {
                System.out.printf("⚠️  DIMINISHING RETURNS: Performance degrades beyond %d parallel operations%n", prev.parallelCount);
                break;
            }
        }
    }

    /**
     * Validates merged splits by running search queries against each one.
     * This ensures the merged splits are not corrupted and contain searchable data.
     */
    private void validateMergedSplitsWithQueries(List<MergeResult> results) throws Exception {
        System.out.println("🔍 Validating " + results.size() + " merged splits with search queries...");

        int validationsPassed = 0;
        int validationsFailed = 0;

        for (MergeResult result : results) {
            try {
                System.out.printf("🔎 Validating split from thread %d: %s%n", result.threadId, result.outputPath);

                // Create SplitSearcher for the merged split
                SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("validation-cache")
                    .withMaxCacheSize(50_000_000); // 50MB for validation
                SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);

                String splitUrl = "file://" + result.outputPath.toAbsolutePath().toString();

                // Read split metadata for proper searcher creation
                QuickwitSplit.SplitMetadata splitMetadata = QuickwitSplit.readSplitMetadata(result.outputPath.toString());

                try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, splitMetadata)) {
                    // Validate split is accessible
                    boolean isValid = searcher.validateSplit();
                    assertTrue(isValid, "Split from thread " + result.threadId + " should be valid and accessible");

                    // Get schema for query construction
                    Schema schema = searcher.getSchema();
                    assertNotNull(schema, "Split from thread " + result.threadId + " should have a valid schema");

                    // Test 1: Count all documents using SplitQuery
                    SplitQuery countQuery = searcher.parseQuery("*");
                    SearchResult countResult = searcher.search(countQuery, 50000); // Large limit to get all docs

                    System.out.printf("   📊 Document count validation: Expected %d, Found %d%n",
                        EXPECTED_MERGED_DOCS, countResult.getHits().size());
                    assertEquals(EXPECTED_MERGED_DOCS, countResult.getHits().size(),
                        "Split from thread " + result.threadId + " should contain exactly " + EXPECTED_MERGED_DOCS + " searchable documents");

                    // Test 2: Search for specific content (assuming test documents contain searchable text)
                    if (schema.hasField("content")) {
                        SplitQuery contentQuery = searcher.parseQuery("content:test");
                        SearchResult contentResult = searcher.search(contentQuery, 100);
                        assertTrue(contentResult.getHits().size() > 0,
                            "Split from thread " + result.threadId + " should contain documents with 'test' content");

                        System.out.printf("   🔍 Content search validation: Found %d documents containing 'test'%n",
                            contentResult.getHits().size());
                    }

                    // Test 3: Try to retrieve actual document data
                    SplitQuery limitedQuery = searcher.parseQuery("*");
                    SearchResult limitedResult = searcher.search(limitedQuery, 5); // Just first 5 docs

                    for (SearchResult.Hit hit : limitedResult.getHits()) {
                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                            assertNotNull(doc, "Documents should be retrievable from split " + result.threadId);

                            // Verify document has some content
                            boolean hasContent = false;
                            for (String fieldName : schema.getFieldNames()) {
                                Object value = doc.getFirst(fieldName);
                                if (value != null) {
                                    hasContent = true;
                                    break;
                                }
                            }
                            assertTrue(hasContent, "Documents should contain field data in split " + result.threadId);
                        }
                    }

                    System.out.printf("   ✅ Document retrieval validation: Successfully retrieved %d documents%n",
                        limitedResult.getHits().size());

                    validationsPassed++;
                    System.out.printf("✅ Thread %d split validation PASSED - All queries successful%n", result.threadId);

                } catch (Exception searchError) {
                    validationsFailed++;
                    System.err.printf("❌ Thread %d split validation FAILED: %s%n", result.threadId, searchError.getMessage());
                    throw new AssertionError("Split validation failed for thread " + result.threadId + ": " + searchError.getMessage(), searchError);
                }

            } catch (Exception e) {
                validationsFailed++;
                System.err.printf("❌ Thread %d split validation FAILED with exception: %s%n", result.threadId, e.getMessage());
                throw new AssertionError("Failed to validate split from thread " + result.threadId, e);
            }
        }

        // Summary
        System.out.printf("🎯 Split validation summary: %d passed, %d failed%n", validationsPassed, validationsFailed);
        assertEquals(results.size(), validationsPassed, "All merged splits should pass validation");
        assertEquals(0, validationsFailed, "No split validations should fail");

        System.out.println("🏆 ALL SPLIT VALIDATIONS PASSED - Merged splits are searchable and contain correct data!");
    }

}
