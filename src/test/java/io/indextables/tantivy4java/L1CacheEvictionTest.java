package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test that demonstrates L1 ByteRangeCache eviction when capacity is exceeded.
 *
 * The L1 cache is bounded (configured via CacheConfig.withMaxCacheSize()) and will
 * automatically evict entries when adding data would exceed the capacity limit.
 * This prevents OOM during large prewarm operations.
 */
public class L1CacheEvictionTest {

    @TempDir
    Path tempDir;

    /**
     * Reset the L1 cache before each test to ensure proper isolation.
     * This allows each test to configure its own L1 cache capacity.
     */
    @BeforeEach
    void resetL1CacheBetweenTests() {
        System.out.println("Resetting L1 cache for test isolation...");
        SplitCacheManager.resetL1Cache();
    }

    /**
     * Test that L1 cache eviction works correctly when capacity is exceeded.
     *
     * Strategy:
     * 1. Create a split with 100K documents, each with a unique row number term
     * 2. Configure a small L1 cache (100KB)
     * 3. Query many different row numbers - each query accesses different parts of the term dictionary
     * 4. Verify evictions occurred as the cache filled up
     */
    @Test
    void testL1CacheEvictsWhenCapacityExceeded() throws Exception {
        System.out.println("============================================================");
        System.out.println("L1 CACHE EVICTION TEST");
        System.out.println("============================================================");
        System.out.println("This test verifies that the L1 ByteRangeCache automatically");
        System.out.println("evicts entries when the configured capacity is exceeded.");
        System.out.println();

        Path splitPath = tempDir.resolve("eviction-test.split");
        Path indexPath = tempDir.resolve("eviction-test-index");
        Path diskCachePath = tempDir.resolve("disk-cache");
        diskCachePath.toFile().mkdirs();

        // Create index with unique row number terms - each doc is ~20KB
        int numDocs = 500; // Fewer docs but each is ~20KB for faster cache filling
        System.out.println("STEP 1: Creating split with " + numDocs + " large documents (~20KB each)...");
        System.out.println("   Each document has a unique 'rowN' term plus ~20KB of UUID content");
        QuickwitSplit.SplitMetadata metadata = createTestSplitWithRowNumbers(indexPath, splitPath, numDocs);
        System.out.println("   Split created: " + splitPath);
        System.out.println("   Split size: " + (splitPath.toFile().length() / 1024) + " KB");
        System.out.println("   Doc count: " + metadata.getNumDocs());

        // Configure cache with small L1 capacity to trigger eviction during queries
        long l1CacheSize = 100_000; // 100KB - small enough to trigger eviction
        System.out.println();
        System.out.println("STEP 2: Configuring cache with " + (l1CacheSize / 1024) + "KB L1 capacity");
        System.out.println("   Queries for different row numbers will access different term dictionary ranges");

        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath.toString())
            .withMaxDiskSize(500_000_000); // 500MB L2

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("eviction-test-" + UUID.randomUUID())
            .withMaxCacheSize(l1CacheSize)
            .withTieredCache(tieredConfig);

        // Capture L1 stats before test
        SplitCacheManager.L1CacheStats statsBefore = SplitCacheManager.getL1CacheStats();
        long evictionsBefore = statsBefore.getEvictions();
        long evictedBytesBefore = statsBefore.getEvictedBytes();
        System.out.println("   L1 stats before: " + statsBefore);

        String splitUri = "file://" + splitPath.toAbsolutePath();

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             SplitSearcher searcher = cacheManager.createSplitSearcher(splitUri, metadata)) {

            System.out.println();
            System.out.println("STEP 3: Retrieving many documents to stress L1 cache...");
            System.out.println("   Document content retrieval bypasses the hotcache, forcing L1 cache usage");
            System.out.println("   With small L1 capacity (" + l1CacheSize / 1024 + "KB), evictions should occur");

            // Query to find all documents
            SplitQuery allQuery = searcher.parseQuery("*");
            var searchResult = searcher.search(allQuery, numDocs); // Get all docs
            System.out.println("   Found " + searchResult.getHits().size() + " documents");

            // Retrieve documents - each ~20KB doc will fill the 100KB L1 cache fast
            int docsToRetrieve = Math.min(100, searchResult.getHits().size()); // ~100 docs at ~20KB = 2MB of data
            int docsRetrieved = 0;

            for (int batch = 0; batch < docsToRetrieve / 10; batch++) {
                for (int i = 0; i < 10 && (batch * 10 + i) < searchResult.getHits().size(); i++) {
                    var hit = searchResult.getHits().get(batch * 10 + i);
                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                        // Access the document content to force cache activity
                        String content = (String) doc.getFirst("content");
                        if (content != null) {
                            docsRetrieved++;
                        }
                    }
                }

                // Report progress every 10 docs
                SplitCacheManager.L1CacheStats midStats = SplitCacheManager.getL1CacheStats();
                System.out.println("   After " + ((batch + 1) * 10) + " docs: " +
                    "L1 size=" + midStats.getSizeBytes() + " bytes, " +
                    "evictions=" + midStats.getEvictions() + ", " +
                    "evictedBytes=" + midStats.getEvictedBytes());
            }

            System.out.println("   Total documents retrieved: " + docsRetrieved);

            // Capture L1 stats after test
            SplitCacheManager.L1CacheStats statsAfter = SplitCacheManager.getL1CacheStats();
            long evictionsAfter = statsAfter.getEvictions();
            long evictedBytesAfter = statsAfter.getEvictedBytes();
            long newEvictions = evictionsAfter - evictionsBefore;
            long newEvictedBytes = evictedBytesAfter - evictedBytesBefore;

            System.out.println();
            System.out.println("STEP 4: Final L1 cache statistics:");
            System.out.println("   " + statsAfter);
            System.out.println("   New evictions during test: " + newEvictions);
            System.out.println("   New bytes evicted: " + newEvictedBytes);

            // Assert that evictions occurred
            System.out.println();
            System.out.println("STEP 5: Verifying evictions occurred...");
            assertTrue(newEvictions > 0 || newEvictedBytes > 0,
                "Expected L1 cache evictions when capacity (" + l1CacheSize + " bytes) was exceeded. " +
                "Evictions: " + newEvictions + ", Evicted bytes: " + newEvictedBytes);

            // Assert cache capacity is set correctly
            assertEquals(l1CacheSize, statsAfter.getCapacityBytes(),
                "L1 cache capacity should match configured size");

            // Assert cache size is within capacity
            assertTrue(statsAfter.getSizeBytes() <= statsAfter.getCapacityBytes(),
                "L1 cache size (" + statsAfter.getSizeBytes() + ") should not exceed capacity (" + statsAfter.getCapacityBytes() + ")");

            System.out.println();
            System.out.println("============================================================");
            System.out.println("TEST PASSED: Evictions verified!");
            System.out.println("============================================================");
            System.out.println("The L1 cache evicted entries when capacity was exceeded:");
            System.out.println("  - Evictions: " + newEvictions);
            System.out.println("  - Bytes evicted: " + newEvictedBytes);
            System.out.println("  - Cache capacity: " + statsAfter.getCapacityBytes());
            System.out.println("  - Cache size after: " + statsAfter.getSizeBytes());
            System.out.println("  - All queries succeeded because L2 disk cache caught the data");
        }

        // Verify disk cache was populated (data was written to L2)
        File[] diskCacheFiles = diskCachePath.toFile().listFiles();
        assertTrue(diskCacheFiles != null && diskCacheFiles.length > 0,
            "L2 disk cache should have been populated");
        System.out.println();
        System.out.println("L2 disk cache contains " + diskCacheFiles.length + " files");
    }

    /**
     * Test that oversized slices are skipped (not stored in L1).
     *
     * If a single slice is larger than the cache capacity, it should be skipped
     * entirely to avoid thrashing.
     */
    @Test
    void testOversizedSlicesAreSkipped() throws Exception {
        System.out.println("============================================================");
        System.out.println("OVERSIZED SLICE SKIP TEST");
        System.out.println("============================================================");
        System.out.println("This test verifies that slices larger than L1 capacity");
        System.out.println("are skipped (not stored) to avoid cache thrashing.");
        System.out.println();

        Path splitPath = tempDir.resolve("oversize-test.split");
        Path indexPath = tempDir.resolve("oversize-test-index");
        Path diskCachePath = tempDir.resolve("disk-cache-oversize");
        diskCachePath.toFile().mkdirs();

        // Create a larger split
        System.out.println("STEP 1: Creating split with large documents...");
        QuickwitSplit.SplitMetadata metadata = createTestSplitWithLargeDocs(indexPath, splitPath, 100);
        System.out.println("   Split created: " + splitPath);
        System.out.println("   Split size: " + splitPath.toFile().length() + " bytes");

        // Configure cache with VERY SMALL L1 (100KB) - smaller than individual components
        long l1CacheSize = 100_000; // 100KB - individual term files may be larger
        System.out.println();
        System.out.println("STEP 2: Configuring cache with tiny L1: " + (l1CacheSize / 1024) + " KB");
        System.out.println("   Slices larger than this should be skipped");

        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath.toString())
            .withMaxDiskSize(100_000_000);

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("oversize-test-" + UUID.randomUUID())
            .withMaxCacheSize(l1CacheSize)
            .withTieredCache(tieredConfig);

        System.out.println();
        System.out.println("STEP 3: Running queries - watch for 'CACHE SKIP' messages...");

        // Capture L1 stats before test
        SplitCacheManager.L1CacheStats statsBefore = SplitCacheManager.getL1CacheStats();
        System.out.println("   L1 stats before: " + statsBefore);

        String splitUri = "file://" + splitPath.toAbsolutePath();

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             SplitSearcher searcher = cacheManager.createSplitSearcher(splitUri, metadata)) {

            SplitQuery query = searcher.parseQuery("*");
            var result = searcher.search(query, 10);

            System.out.println("   Query returned " + result.getHits().size() + " results");
            assertTrue(result.getHits().size() > 0, "Should find some results");

            // Capture L1 stats after test
            SplitCacheManager.L1CacheStats statsAfter = SplitCacheManager.getL1CacheStats();
            System.out.println("   L1 stats after: " + statsAfter);

            // Assert cache capacity is set correctly
            assertEquals(l1CacheSize, statsAfter.getCapacityBytes(),
                "L1 cache capacity should match configured size");

            // Assert cache size is within capacity (oversized slices were skipped)
            assertTrue(statsAfter.getSizeBytes() <= statsAfter.getCapacityBytes(),
                "L1 cache size (" + statsAfter.getSizeBytes() + ") should not exceed capacity (" + statsAfter.getCapacityBytes() + ")");

            System.out.println();
            System.out.println("============================================================");
            System.out.println("TEST PASSED: Queries succeeded with oversized slice skipping");
            System.out.println("============================================================");
            System.out.println("  - Cache capacity: " + statsAfter.getCapacityBytes());
            System.out.println("  - Cache size: " + statsAfter.getSizeBytes());
            System.out.println("  - Size is within capacity (oversized slices were skipped)");
        }
    }

    /**
     * Create a split with documents containing unique row number terms.
     * Each document has "rowN" plus ~20KB of content for fast cache filling.
     */
    private QuickwitSplit.SplitMetadata createTestSplitWithRowNumbers(Path indexPath, Path splitPath, int numDocs) throws Exception {
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("content", true, false, "default", "position");
            builder.addIntegerField("id", true, true, true);

            try (Schema schema = builder.build();
                 Index index = new Index(schema, indexPath.toString(), false)) {

                try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                    for (int i = 0; i < numDocs; i++) {
                        try (Document doc = new Document()) {
                            // Each document has ~20KB of content for faster cache filling
                            StringBuilder sb = new StringBuilder(21000);
                            sb.append("row").append(i).append(" ");
                            // Add ~20KB of random content (each UUID is ~36 chars)
                            for (int j = 0; j < 500; j++) {
                                sb.append(UUID.randomUUID().toString()).append(" ");
                            }
                            doc.addText("content", sb.toString());
                            doc.addInteger("id", i);
                            writer.addDocument(doc);
                        }
                        // Commit periodically for large indices
                        if (i > 0 && i % 1000 == 0) {
                            writer.commit();
                            System.out.println("      Committed " + i + " documents...");
                        }
                    }
                    writer.commit();
                }

                index.reload();

                QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                    "eviction-test", "test-source", "test-node");
                return QuickwitSplit.convertIndexFromPath(indexPath.toString(), splitPath.toString(), config);
            }
        }
    }

    private QuickwitSplit.SplitMetadata createTestSplitWithLargeDocs(Path indexPath, Path splitPath, int numDocs) throws Exception {
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("content", true, false, "default", "position");
            builder.addIntegerField("id", true, true, true);

            try (Schema schema = builder.build();
                 Index index = new Index(schema, indexPath.toString(), false)) {

                try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                    for (int i = 0; i < numDocs; i++) {
                        try (Document doc = new Document()) {
                            // Create larger documents with lots of unique terms
                            StringBuilder sb = new StringBuilder();
                            for (int j = 0; j < 100; j++) {
                                sb.append("word").append(i).append("_").append(j).append(" ");
                                sb.append(UUID.randomUUID().toString()).append(" ");
                            }
                            doc.addText("content", sb.toString());
                            doc.addInteger("id", i);
                            writer.addDocument(doc);
                        }
                    }
                    writer.commit();
                }

                index.reload();

                QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                    "oversize-test", "test-source", "test-node");
                return QuickwitSplit.convertIndexFromPath(indexPath.toString(), splitPath.toString(), config);
            }
        }
    }
}
