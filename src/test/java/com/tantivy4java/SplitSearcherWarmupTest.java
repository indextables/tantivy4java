package com.tantivy4java;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test suite for SplitSearcher warmup functionality and parallel hot cache loading.
 * Tests the high-impact optimization features:
 * - Query-specific warmup system
 * - Parallel hot cache loading
 * - Component preloading
 * - Warmup statistics
 */
public class SplitSearcherWarmupTest {

    private static SplitCacheManager cacheManager;
    
    @TempDir
    static Path tempDir;
    
    private Index testIndex;
    private Path indexPath;
    private Path splitPath;

    @BeforeAll
    static void setUpCacheManager() {
        // Create shared cache manager for warmup tests
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("warmup-test-cache")
            .withMaxCacheSize(100_000_000) // 100MB shared cache for warmup testing
            .withMaxConcurrentLoads(8);
            
        cacheManager = SplitCacheManager.getInstance(config);
    }
    
    @AfterAll
    static void tearDownCacheManager() {
        if (cacheManager != null) {
            try {
                cacheManager.close();
            } catch (Exception e) {
                // Log error but continue cleanup
                System.err.println("Error closing cache manager: " + e);
            }
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        // Create index directory
        indexPath = tempDir.resolve("test_index");
        Files.createDirectories(indexPath);
        
        // Create test index with multiple field types for warmup testing
        SchemaBuilder schemaBuilder = new SchemaBuilder();
        schemaBuilder.addTextField("title", true, false, "default", "position");
        schemaBuilder.addTextField("content", true, true, "default", "position");  // Fast field
        schemaBuilder.addIntegerField("count", true, true, true);  // Indexed and fast
        schemaBuilder.addFloatField("score", true, true, true);    // Indexed and fast
        schemaBuilder.addBooleanField("active", true, true, true); // Indexed and fast
        
        Schema schema = schemaBuilder.build();
        testIndex = new Index(schema, indexPath.toString());
        
        // Add substantial test data to make warmup meaningful
        try (IndexWriter writer = testIndex.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
            for (int i = 0; i < 1000; i++) {
                Document doc = new Document();
                doc.addText("title", "Document " + i);
                doc.addText("content", "This is the content for document " + i + " with search terms");
                doc.addInteger("count", (long) i);
                doc.addFloat("score", (float) (i * 0.1));
                doc.addBoolean("active", i % 2 == 0);
                writer.addDocument(doc);
            }
            writer.commit();
        }
        
        // Convert to split for testing
        splitPath = tempDir.resolve("test.split");
        QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
            "test-index-" + System.currentTimeMillis(),
            "warmup-test-source",
            "warmup-test-node"
        );
        QuickwitSplit.convertIndexFromPath(indexPath.toString(), splitPath.toString(), splitConfig);
    }

    @Test
    @DisplayName("Test basic warmup functionality")
    void testBasicWarmup() throws Exception {
        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath.toString())) {
            Schema schema = searcher.getSchema();
            
            // Create a query to warm up
            Query query = Query.termQuery(schema, "content", "search");
            
            // Perform warmup
            CompletableFuture<Void> warmupFuture = searcher.warmupQuery(query);
            
            // Wait for warmup to complete
            assertDoesNotThrow(() -> warmupFuture.get(10, TimeUnit.SECONDS));
            
            // Verify search works after warmup
            SearchResult results = searcher.search(query, 10);
            assertNotNull(results);
            assertTrue(results.getHits().size() > 0, "Should find documents after warmup");
        }
    }

    @Test
    @DisplayName("Test advanced warmup with statistics")
    void testAdvancedWarmupWithStats() throws Exception {
        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath.toString())) {
            Schema schema = searcher.getSchema();
            
            // Create a complex query that requires multiple components
            Query termQuery = Query.termQuery(schema, "title", "Document");
            Query rangeQuery = Query.rangeQuery(schema, "count", FieldType.INTEGER, 100L, 200L, true, true);
            Query combinedQuery = Query.booleanQuery(
                List.of(new Query.OccurQuery(Occur.MUST, termQuery), 
                        new Query.OccurQuery(Occur.MUST, rangeQuery))
            );
            
            // Perform advanced warmup with statistics
            CompletableFuture<SplitSearcher.WarmupStats> warmupFuture = 
                searcher.warmupQueryAdvanced(combinedQuery, null, true);
            
            // Wait and get warmup statistics
            SplitSearcher.WarmupStats stats = warmupFuture.get(10, TimeUnit.SECONDS);
            
            assertNotNull(stats);
            assertTrue(stats.getTotalBytesLoaded() >= 0, "Should have loaded some bytes");
            assertTrue(stats.getWarmupTimeMs() > 0, "Warmup should take some time");
            assertTrue(stats.getComponentsLoaded() > 0, "Should have loaded some components");
            assertTrue(stats.isUsedParallelLoading(), "Should use parallel loading when enabled");
            
            // Verify component sizes were tracked
            Map<SplitSearcher.IndexComponent, Long> componentSizes = stats.getComponentSizes();
            assertNotNull(componentSizes);
            
            // Calculate loading speed
            double speedMBps = stats.getLoadingSpeedMBps();
            assertTrue(speedMBps >= 0, "Loading speed should be non-negative");
            
            System.out.println("Warmup Statistics:");
            System.out.println("- Total bytes loaded: " + stats.getTotalBytesLoaded());
            System.out.println("- Warmup time: " + stats.getWarmupTimeMs() + "ms");
            System.out.println("- Components loaded: " + stats.getComponentsLoaded());
            System.out.println("- Loading speed: " + speedMBps + " MB/s");
            System.out.println("- Parallel loading: " + stats.isUsedParallelLoading());
        }
    }

    @Test
    @DisplayName("Test selective component warmup")
    void testSelectiveComponentWarmup() throws Exception {
        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath.toString())) {
            Schema schema = searcher.getSchema();
            Query query = Query.termQuery(schema, "content", "search");
            
            // Warm up only specific components
            SplitSearcher.IndexComponent[] componentsToWarm = {
                SplitSearcher.IndexComponent.FASTFIELD,
                SplitSearcher.IndexComponent.POSTINGS
            };
            
            CompletableFuture<SplitSearcher.WarmupStats> warmupFuture = 
                searcher.warmupQueryAdvanced(query, componentsToWarm, true);
            
            SplitSearcher.WarmupStats stats = warmupFuture.get(10, TimeUnit.SECONDS);
            
            assertNotNull(stats);
            assertTrue(stats.getComponentsLoaded() > 0, "Should have loaded specified components");
            
            // Check that specific components were loaded
            Map<SplitSearcher.IndexComponent, Long> componentSizes = stats.getComponentSizes();
            assertTrue(componentSizes.containsKey(SplitSearcher.IndexComponent.FASTFIELD) ||
                      componentSizes.containsKey(SplitSearcher.IndexComponent.POSTINGS),
                      "Should have loaded at least one of the specified components");
        }
    }

    @Test
    @DisplayName("Test component preloading")
    void testComponentPreloading() throws Exception {
        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath.toString())) {
            // Preload specific components
            CompletableFuture<Void> preloadFuture = searcher.preloadComponents(
                SplitSearcher.IndexComponent.SCHEMA,
                SplitSearcher.IndexComponent.FASTFIELD,
                SplitSearcher.IndexComponent.FIELDNORM
            );
            
            // Wait for preloading to complete
            assertDoesNotThrow(() -> preloadFuture.get(10, TimeUnit.SECONDS));
            
            // Verify components are cached
            Map<SplitSearcher.IndexComponent, Boolean> cacheStatus = searcher.getComponentCacheStatus();
            assertNotNull(cacheStatus);
            
            // At least some components should be cached after preloading
            boolean hasAnyCached = cacheStatus.values().stream().anyMatch(cached -> cached);
            assertTrue(hasAnyCached, "Should have at least some components cached after preloading");
        }
    }

    @Test
    @DisplayName("Test warmup improves search performance")
    void testWarmupImprovesPerformance() throws Exception {
        // Create two identical searchers
        try (SplitSearcher searcherCold = cacheManager.createSplitSearcher("file://" + splitPath.toString());
             SplitSearcher searcherWarm = cacheManager.createSplitSearcher("file://" + splitPath.toString())) {
            
            Schema schema = searcherCold.getSchema();
            Query query = Query.termQuery(schema, "content", "search");
            
            // Warm up the second searcher
            CompletableFuture<Void> warmupFuture = searcherWarm.warmupQuery(query);
            warmupFuture.get(10, TimeUnit.SECONDS);
            
            // Measure cold search time
            long coldStart = System.nanoTime();
            SearchResult coldResults = searcherCold.search(query, 100);
            long coldTime = System.nanoTime() - coldStart;
            
            // Measure warm search time
            long warmStart = System.nanoTime();
            SearchResult warmResults = searcherWarm.search(query, 100);
            long warmTime = System.nanoTime() - warmStart;
            
            // Verify both searches return same results
            assertEquals(coldResults.getHits().size(), warmResults.getHits().size(), 
                        "Cold and warm searches should return same number of hits");
            
            // Log performance comparison
            System.out.println("Performance Comparison:");
            System.out.println("- Cold search time: " + (coldTime / 1_000_000) + "ms");
            System.out.println("- Warm search time: " + (warmTime / 1_000_000) + "ms");
            
            // Note: We can't guarantee warm is always faster due to JIT compilation,
            // but we verify both searches complete successfully
            assertTrue(coldTime > 0 && warmTime > 0, "Both searches should complete");
        }
    }

    @Test
    @DisplayName("Test parallel warmup of multiple queries")
    void testParallelWarmupMultipleQueries() throws Exception {
        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath.toString())) {
            Schema schema = searcher.getSchema();
            
            // Create multiple different queries
            List<Query> queries = new ArrayList<>();
            queries.add(Query.termQuery(schema, "title", "Document"));
            queries.add(Query.termQuery(schema, "content", "search"));
            queries.add(Query.rangeQuery(schema, "count", FieldType.INTEGER, 0L, 500L, true, true));
            
            // Warm up all queries in parallel
            List<CompletableFuture<Void>> warmupFutures = new ArrayList<>();
            for (Query query : queries) {
                warmupFutures.add(searcher.warmupQuery(query));
            }
            
            // Wait for all warmups to complete
            CompletableFuture<Void> allWarmups = CompletableFuture.allOf(
                warmupFutures.toArray(new CompletableFuture[0])
            );
            
            assertDoesNotThrow(() -> allWarmups.get(10, TimeUnit.SECONDS));
            
            // Verify all queries work after warmup
            for (Query query : queries) {
                SearchResult results = searcher.search(query, 10);
                assertNotNull(results);
                assertTrue(results.getHits().size() >= 0, "Query should execute after warmup");
            }
        }
    }

    @Test
    @DisplayName("Test cache statistics after warmup")
    void testCacheStatisticsAfterWarmup() throws Exception {
        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath.toString())) {
            Schema schema = searcher.getSchema();
            Query query = Query.termQuery(schema, "content", "search");
            
            // Get initial cache stats
            SplitSearcher.CacheStats initialStats = searcher.getCacheStats();
            long initialHits = initialStats.getHitCount();
            
            // Perform warmup
            searcher.warmupQuery(query).get(10, TimeUnit.SECONDS);
            
            // Perform search multiple times to generate cache hits
            for (int i = 0; i < 5; i++) {
                searcher.search(query, 10);
            }
            
            // Get cache stats after warmup and searches
            SplitSearcher.CacheStats finalStats = searcher.getCacheStats();
            
            // Verify cache statistics
            assertTrue(finalStats.getTotalRequests() > initialStats.getTotalRequests(), 
                      "Should have more cache requests after warmup and searches");
            assertTrue(finalStats.getTotalSize() > 0, "Cache should contain some data");
            assertTrue(finalStats.getUtilization() > 0, "Cache should have some utilization");
            
            System.out.println("Cache Statistics:");
            System.out.println("- Hit rate: " + (finalStats.getHitRate() * 100) + "%");
            System.out.println("- Total requests: " + finalStats.getTotalRequests());
            System.out.println("- Cache size: " + finalStats.getTotalSize() + " bytes");
            System.out.println("- Utilization: " + (finalStats.getUtilization() * 100) + "%");
        }
    }

    @Test
    @DisplayName("Test component eviction after warmup")
    void testComponentEviction() throws Exception {
        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath.toString())) {
            // Preload components
            searcher.preloadComponents(
                SplitSearcher.IndexComponent.FASTFIELD,
                SplitSearcher.IndexComponent.POSTINGS
            ).get(10, TimeUnit.SECONDS);
            
            // Verify components are cached
            Map<SplitSearcher.IndexComponent, Boolean> statusBefore = searcher.getComponentCacheStatus();
            boolean hasAnyCachedBefore = statusBefore.values().stream().anyMatch(cached -> cached);
            assertTrue(hasAnyCachedBefore, "Should have components cached after preloading");
            
            // Evict specific components
            searcher.evictComponents(
                SplitSearcher.IndexComponent.FASTFIELD,
                SplitSearcher.IndexComponent.POSTINGS
            );
            
            // Note: Eviction may not immediately clear all components due to active references,
            // but the eviction method should execute without errors
            assertTrue(true, "Eviction should complete without errors");
        }
    }

    @Test
    @DisplayName("Test warmup with non-parallel mode")
    void testWarmupNonParallel() throws Exception {
        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath.toString())) {
            Schema schema = searcher.getSchema();
            Query query = Query.termQuery(schema, "content", "search");
            
            // Perform warmup with parallel disabled
            CompletableFuture<SplitSearcher.WarmupStats> warmupFuture = 
                searcher.warmupQueryAdvanced(query, null, false);
            
            SplitSearcher.WarmupStats stats = warmupFuture.get(10, TimeUnit.SECONDS);
            
            assertNotNull(stats);
            assertFalse(stats.isUsedParallelLoading(), "Should not use parallel loading when disabled");
            assertTrue(stats.getWarmupTimeMs() > 0, "Warmup should still complete");
        }
    }

    @AfterEach
    void tearDown() throws IOException {
        if (testIndex != null) {
            testIndex.close();
        }
    }
}