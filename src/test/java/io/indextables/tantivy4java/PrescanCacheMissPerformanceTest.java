package io.indextables.tantivy4java;

import static org.junit.jupiter.api.Assertions.*;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.QuickwitSplit;
import org.junit.jupiter.api.*;

import java.nio.file.*;
import java.util.*;

/**
 * Performance test for prescan cache behavior.
 *
 * Tests three scenarios:
 * 1. Memory cache HIT - data already in memory LRU
 * 2. Disk cache HIT - data on disk, needs to be loaded into memory
 * 3. Complete cache MISS - data needs to be fetched from storage
 *
 * This helps understand the performance impact of cache misses.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PrescanCacheMissPerformanceTest {

    private static Path tempDir;
    private static Path diskCacheDir;
    private static List<SplitInfo> splits = new ArrayList<>();
    private static List<String> splitPaths = new ArrayList<>();
    private static String docMappingJson;

    // Create 10 splits to test cache eviction scenarios
    private static final int NUM_SPLITS = 10;

    @BeforeAll
    static void setUp() throws Exception {
        tempDir = Files.createTempDirectory("prescan-cache-miss-perf");
        diskCacheDir = tempDir.resolve("disk_cache");
        Files.createDirectories(diskCacheDir);

        // Create multiple splits with different data
        for (int splitIdx = 0; splitIdx < NUM_SPLITS; splitIdx++) {
            Path indexDir = tempDir.resolve("index_" + splitIdx);
            Files.createDirectories(indexDir);
            Path splitFile = tempDir.resolve("split_" + splitIdx + ".split");

            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder.addTextField("title");
                builder.addTextField("body");
                builder.addTextField("category");

                try (Schema schema = builder.build()) {
                    try (Index index = new Index(schema, indexDir.toString(), false)) {
                        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                            // Add documents with split-specific terms
                            for (int docIdx = 0; docIdx < 100; docIdx++) {
                                try (Document d = new Document()) {
                                    d.addText("title", "split" + splitIdx + " doc" + docIdx + " hello world");
                                    d.addText("body", "content for split " + splitIdx + " document " + docIdx);
                                    d.addText("category", "category" + (docIdx % 5));
                                    writer.addDocument(d);
                                }
                            }
                            writer.commit();
                        }

                        // Convert to split
                        QuickwitSplit.SplitConfig config =
                            new QuickwitSplit.SplitConfig("cache-test-index", "source", "node");
                        QuickwitSplit.SplitMetadata metadata =
                            QuickwitSplit.convertIndexFromPath(indexDir.toString(), splitFile.toString(), config);

                        String localPath = "file://" + splitFile.toString();
                        splitPaths.add(localPath);
                        splits.add(new SplitInfo(localPath, metadata.getFooterStartOffset(), metadata.getFooterEndOffset()));

                        if (docMappingJson == null) {
                            docMappingJson = metadata.getDocMappingJson();
                        }
                    }
                }
            }
        }

        System.out.println("Created " + NUM_SPLITS + " test splits");
        System.out.println("Disk cache directory: " + diskCacheDir);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (tempDir != null) {
            Files.walk(tempDir)
                .sorted(Comparator.reverseOrder())
                .forEach(p -> {
                    try { Files.deleteIfExists(p); } catch (Exception e) {}
                });
        }
    }

    @Test
    @Order(1)
    void testMemoryCacheHitPerformance() throws Exception {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("TEST 1: MEMORY CACHE HIT PERFORMANCE");
        System.out.println("=".repeat(70));

        // Large memory cache - everything fits
        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig("memory-hit-test")
                .withMaxCacheSize(500_000_000); // 500MB - plenty of room

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
            SplitQuery query = new SplitTermQuery("title", "hello");

            // Warmup - populate cache
            System.out.println("Warming up (populating memory cache)...");
            for (int i = 0; i < 50; i++) {
                for (SplitInfo split : splits) {
                    cacheManager.prescanSplits(List.of(split), docMappingJson, query);
                }
            }

            // Now everything should be in memory cache
            final int ITERATIONS = 10000;
            System.out.println("Running " + ITERATIONS + " iterations (memory cache hits)...");

            long start = System.nanoTime();
            for (int i = 0; i < ITERATIONS; i++) {
                SplitInfo split = splits.get(i % NUM_SPLITS);
                cacheManager.prescanSplits(List.of(split), docMappingJson, query);
            }
            long end = System.nanoTime();

            double avgMicros = (end - start) / 1000.0 / ITERATIONS;
            double throughput = ITERATIONS * 1000.0 / ((end - start) / 1_000_000.0);

            System.out.println("\n" + "-".repeat(50));
            System.out.println("MEMORY CACHE HIT RESULTS:");
            System.out.printf("  Total time:     %.2f ms%n", (end - start) / 1_000_000.0);
            System.out.printf("  Avg per call:   %.2f µs%n", avgMicros);
            System.out.printf("  Throughput:     %.0f prescans/sec%n", throughput);
            System.out.println("-".repeat(50));
        }
    }

    @Test
    @Order(2)
    void testDiskCacheHitPerformance() throws Exception {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("TEST 2: DISK CACHE HIT PERFORMANCE (LRU eviction + disk read)");
        System.out.println("=".repeat(70));

        // Very small memory cache - forces disk reads
        // Each term dict is roughly 1-5KB, so 10KB can hold maybe 2-5 entries
        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig("disk-hit-test")
                .withMaxCacheSize(10_000); // 10KB - forces eviction

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
            SplitQuery query = new SplitTermQuery("title", "hello");

            // First pass - populates disk cache
            System.out.println("First pass - populating disk cache...");
            for (SplitInfo split : splits) {
                cacheManager.prescanSplits(List.of(split), docMappingJson, query);
            }

            // Small warmup
            System.out.println("Warmup...");
            for (int i = 0; i < 20; i++) {
                for (SplitInfo split : splits) {
                    cacheManager.prescanSplits(List.of(split), docMappingJson, query);
                }
            }

            // Measure with forced cache eviction pattern
            // Access splits in round-robin to force evictions
            final int ITERATIONS = 5000;
            System.out.println("Running " + ITERATIONS + " iterations (disk cache hits, memory evictions)...");

            long start = System.nanoTime();
            for (int i = 0; i < ITERATIONS; i++) {
                SplitInfo split = splits.get(i % NUM_SPLITS);
                cacheManager.prescanSplits(List.of(split), docMappingJson, query);
            }
            long end = System.nanoTime();

            double avgMicros = (end - start) / 1000.0 / ITERATIONS;
            double throughput = ITERATIONS * 1000.0 / ((end - start) / 1_000_000.0);

            System.out.println("\n" + "-".repeat(50));
            System.out.println("DISK CACHE HIT RESULTS (with memory eviction):");
            System.out.printf("  Total time:     %.2f ms%n", (end - start) / 1_000_000.0);
            System.out.printf("  Avg per call:   %.2f µs%n", avgMicros);
            System.out.printf("  Throughput:     %.0f prescans/sec%n", throughput);
            System.out.println("-".repeat(50));
        }
    }

    @Test
    @Order(3)
    void testColdStartPerformance() throws Exception {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("TEST 3: COLD START PERFORMANCE (no cache)");
        System.out.println("=".repeat(70));

        // Each iteration creates a new cache manager with unique name
        // This simulates cold start with no cached data
        SplitQuery query = new SplitTermQuery("title", "hello");

        final int ITERATIONS = 100; // Fewer iterations since each is expensive
        System.out.println("Running " + ITERATIONS + " cold start iterations...");

        List<Long> timings = new ArrayList<>();

        for (int i = 0; i < ITERATIONS; i++) {
            // New cache manager each time - completely cold
            SplitCacheManager.CacheConfig cacheConfig =
                new SplitCacheManager.CacheConfig("cold-start-" + i + "-" + System.nanoTime())
                    .withMaxCacheSize(100_000_000);

            long start = System.nanoTime();
            try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
                SplitInfo split = splits.get(i % NUM_SPLITS);
                cacheManager.prescanSplits(List.of(split), docMappingJson, query);
            }
            long end = System.nanoTime();

            timings.add(end - start);
        }

        // Calculate statistics
        Collections.sort(timings);
        long sum = timings.stream().mapToLong(Long::longValue).sum();
        double avgMicros = sum / 1000.0 / ITERATIONS;
        double p50Micros = timings.get(ITERATIONS / 2) / 1000.0;
        double p99Micros = timings.get((int)(ITERATIONS * 0.99)) / 1000.0;
        double minMicros = timings.get(0) / 1000.0;
        double maxMicros = timings.get(ITERATIONS - 1) / 1000.0;

        System.out.println("\n" + "-".repeat(50));
        System.out.println("COLD START RESULTS:");
        System.out.printf("  Avg:    %.2f µs%n", avgMicros);
        System.out.printf("  P50:    %.2f µs%n", p50Micros);
        System.out.printf("  P99:    %.2f µs%n", p99Micros);
        System.out.printf("  Min:    %.2f µs%n", minMicros);
        System.out.printf("  Max:    %.2f µs%n", maxMicros);
        System.out.println("-".repeat(50));
    }

    @Test
    @Order(4)
    void testCacheEvictionPattern() throws Exception {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("TEST 4: CACHE EVICTION PATTERN ANALYSIS");
        System.out.println("=".repeat(70));

        // Moderate cache - can hold about half the splits
        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig("eviction-pattern-test")
                .withMaxCacheSize(50_000); // ~50KB

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
            SplitQuery query = new SplitTermQuery("title", "hello");

            // Warmup
            for (int i = 0; i < 10; i++) {
                for (SplitInfo split : splits) {
                    cacheManager.prescanSplits(List.of(split), docMappingJson, query);
                }
            }

            final int ITERATIONS = 1000;

            // Pattern 1: Sequential access (worst for LRU)
            System.out.println("\nPattern 1: Sequential access (worst case for LRU)");
            long seqStart = System.nanoTime();
            for (int i = 0; i < ITERATIONS; i++) {
                SplitInfo split = splits.get(i % NUM_SPLITS);
                cacheManager.prescanSplits(List.of(split), docMappingJson, query);
            }
            long seqEnd = System.nanoTime();
            double seqAvgMicros = (seqEnd - seqStart) / 1000.0 / ITERATIONS;

            // Pattern 2: Hot/cold - 80% access to first 2 splits
            System.out.println("Pattern 2: Hot/cold (80% to 2 splits, 20% to rest)");
            long hotColdStart = System.nanoTime();
            Random rand = new Random(42);
            for (int i = 0; i < ITERATIONS; i++) {
                int splitIdx;
                if (rand.nextDouble() < 0.8) {
                    splitIdx = rand.nextInt(2); // Hot splits (0-1)
                } else {
                    splitIdx = 2 + rand.nextInt(NUM_SPLITS - 2); // Cold splits
                }
                SplitInfo split = splits.get(splitIdx);
                cacheManager.prescanSplits(List.of(split), docMappingJson, query);
            }
            long hotColdEnd = System.nanoTime();
            double hotColdAvgMicros = (hotColdEnd - hotColdStart) / 1000.0 / ITERATIONS;

            // Pattern 3: Random access
            System.out.println("Pattern 3: Random access");
            long randomStart = System.nanoTime();
            for (int i = 0; i < ITERATIONS; i++) {
                SplitInfo split = splits.get(rand.nextInt(NUM_SPLITS));
                cacheManager.prescanSplits(List.of(split), docMappingJson, query);
            }
            long randomEnd = System.nanoTime();
            double randomAvgMicros = (randomEnd - randomStart) / 1000.0 / ITERATIONS;

            System.out.println("\n" + "-".repeat(50));
            System.out.println("EVICTION PATTERN RESULTS:");
            System.out.printf("  Sequential (worst):   %.2f µs/call%n", seqAvgMicros);
            System.out.printf("  Hot/cold (80/20):     %.2f µs/call%n", hotColdAvgMicros);
            System.out.printf("  Random:               %.2f µs/call%n", randomAvgMicros);
            System.out.printf("  Hot/cold speedup:     %.1fx faster than sequential%n",
                             seqAvgMicros / hotColdAvgMicros);
            System.out.println("-".repeat(50));
        }
    }

    @Test
    @Order(5)
    void testComparisonSummary() throws Exception {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("TEST 5: COMPREHENSIVE COMPARISON SUMMARY");
        System.out.println("=".repeat(70));

        SplitQuery query = new SplitTermQuery("title", "hello");
        final int ITERATIONS = 2000;

        // Test 1: Memory cache hit (large cache)
        SplitCacheManager.CacheConfig memConfig =
            new SplitCacheManager.CacheConfig("summary-mem-" + System.nanoTime())
                .withMaxCacheSize(500_000_000);

        double memAvg;
        try (SplitCacheManager cm = SplitCacheManager.getInstance(memConfig)) {
            // Warmup
            for (int i = 0; i < 100; i++) {
                for (SplitInfo split : splits) {
                    cm.prescanSplits(List.of(split), docMappingJson, query);
                }
            }
            // Measure
            long start = System.nanoTime();
            for (int i = 0; i < ITERATIONS; i++) {
                cm.prescanSplits(List.of(splits.get(i % NUM_SPLITS)), docMappingJson, query);
            }
            long end = System.nanoTime();
            memAvg = (end - start) / 1000.0 / ITERATIONS;
        }

        // Test 2: Disk cache hit (tiny memory cache)
        SplitCacheManager.CacheConfig diskConfig =
            new SplitCacheManager.CacheConfig("summary-disk-" + System.nanoTime())
                .withMaxCacheSize(10_000);

        double diskAvg;
        try (SplitCacheManager cm = SplitCacheManager.getInstance(diskConfig)) {
            // Warmup to populate disk
            for (int i = 0; i < 50; i++) {
                for (SplitInfo split : splits) {
                    cm.prescanSplits(List.of(split), docMappingJson, query);
                }
            }
            // Measure
            long start = System.nanoTime();
            for (int i = 0; i < ITERATIONS; i++) {
                cm.prescanSplits(List.of(splits.get(i % NUM_SPLITS)), docMappingJson, query);
            }
            long end = System.nanoTime();
            diskAvg = (end - start) / 1000.0 / ITERATIONS;
        }

        // Print comparison
        System.out.println("\n╔════════════════════════════════════════════════════════════════╗");
        System.out.println("║           CACHE PERFORMANCE COMPARISON SUMMARY                  ║");
        System.out.println("╠════════════════════════════════════════════════════════════════╣");
        System.out.printf("║  Memory Cache Hit:     %8.2f µs/call                        ║%n", memAvg);
        System.out.printf("║  Disk Cache Hit:       %8.2f µs/call                        ║%n", diskAvg);
        System.out.println("╠════════════════════════════════════════════════════════════════╣");
        System.out.printf("║  Disk/Memory Ratio:    %8.1fx slower                        ║%n", diskAvg / memAvg);
        System.out.println("╠════════════════════════════════════════════════════════════════╣");
        System.out.printf("║  Memory Throughput:    %8.0f prescans/sec                   ║%n", 1_000_000.0 / memAvg);
        System.out.printf("║  Disk Throughput:      %8.0f prescans/sec                   ║%n", 1_000_000.0 / diskAvg);
        System.out.println("╚════════════════════════════════════════════════════════════════╝");
    }
}
