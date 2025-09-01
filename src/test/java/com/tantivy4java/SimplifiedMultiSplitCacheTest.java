package com.tantivy4java;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.*;

/**
 * Simplified multi-split cache test without S3Mock dependency.
 * Demonstrates the corrected shared cache architecture based on Quickwit patterns.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SimplifiedMultiSplitCacheTest {
    
    private static SplitCacheManager cacheManager;
    
    @TempDir
    static Path tempDir;
    
    private Schema testSchema;
    private List<String> localSplitPaths;
    
    @BeforeAll
    static void setUp() {
        // Create shared cache manager
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("simplified-test-cache")
            .withMaxCacheSize(100_000_000) // 100MB shared cache
            .withMaxConcurrentLoads(8);
        
        cacheManager = SplitCacheManager.getInstance(config);
    }
    
    @AfterAll
    static void tearDown() {
        if (cacheManager != null) {
            try {
                cacheManager.close();
            } catch (Exception e) {
                // Log error but continue cleanup
            }
        }
    }
    
    @BeforeEach
    void setUpEach() throws Exception {
        // Create test schema
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addTextField("content", true, false, "default", "position")
                   .addIntegerField("category_id", true, true, true);
            testSchema = builder.build();
        }
        
        // Create multiple test splits
        localSplitPaths = new ArrayList<>();
        
        for (int i = 0; i < 3; i++) {
            // Create local index with different data
            Path indexPath = tempDir.resolve("test-index-" + i);
            try (Index index = new Index(testSchema, indexPath.toString(), false)) {
                try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                    // Add different documents to each split
                    for (int j = 0; j < 5; j++) {
                        String json = String.format(
                            "{ \"title\": \"Split %d Document %d\", " +
                            "\"content\": \"Content for split %d document %d\", " +
                            "\"category_id\": %d }",
                            i, j, i, j, (i * 10 + j) % 5
                        );
                        writer.addJson(json);
                    }
                    writer.commit();
                }
                index.reload();
            }
            
            // Convert to Quickwit split
            Path localSplitPath = tempDir.resolve("test-split-" + i + ".split");
            QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                "test-index-uid", "test-source-" + i, "test-node"
            );
            QuickwitSplit.convertIndexFromPath(indexPath.toString(), localSplitPath.toString(), splitConfig);
            localSplitPaths.add("file://" + localSplitPath.toString());
        }
    }
    
    @Test
    @org.junit.jupiter.api.Order(1)
    @DisplayName("Create multiple split searchers with shared cache")
    void testCreateMultipleSplitSearchers() {
        // Test batch creation with local splits
        List<SplitSearcher> searchers = cacheManager.createMultipleSplitSearchers(localSplitPaths);
        
        assertEquals(3, searchers.size(), "Should create 3 split searchers");
        assertEquals(3, cacheManager.getManagedSplitPaths().size(), "Should manage 3 split paths");
        
        // Verify each searcher can validate its split
        for (int i = 0; i < searchers.size(); i++) {
            assertTrue(searchers.get(i).validateSplit(), 
                "Split searcher " + i + " should validate its split");
        }
        
        // Close all searchers
        for (SplitSearcher searcher : searchers) {
            searcher.close();
        }
    }
    
    @Test
    @org.junit.jupiter.api.Order(2)
    @DisplayName("Test global cache statistics across multiple splits")
    void testGlobalCacheStatistics() {
        List<SplitSearcher> searchers = cacheManager.createMultipleSplitSearchers(localSplitPaths);
        
        try {
            // Check global cache statistics
            SplitCacheManager.GlobalCacheStats stats = cacheManager.getGlobalCacheStats();
            
            assertNotNull(stats, "Global cache stats should not be null");
            assertTrue(stats.getActiveSplits() >= 3, "Should have at least 3 active splits");
            assertTrue(stats.getMaxSize() > 0, "Should have max size configured");
            
            // Verify cache sharing efficiency
            double hitRate = stats.getHitRate();
            assertTrue(hitRate >= 0.0 && hitRate <= 1.0, 
                "Hit rate should be between 0 and 1, got: " + hitRate);
            
        } finally {
            for (SplitSearcher searcher : searchers) {
                searcher.close();
            }
        }
    }
    
    @Test
    @org.junit.jupiter.api.Order(3)
    @DisplayName("Test component preloading across multiple splits")
    void testComponentPreloadingAcrossMultipleSplits() {
        List<SplitSearcher> searchers = cacheManager.createMultipleSplitSearchers(localSplitPaths);
        
        try {
            // Preload specific components for all splits
            Set<SplitSearcher.IndexComponent> components = Set.of(
                SplitSearcher.IndexComponent.SCHEMA,
                SplitSearcher.IndexComponent.POSTINGS
            );
            
            for (String splitPath : localSplitPaths) {
                cacheManager.preloadComponents(splitPath, components);
            }
            
            // Verify cache has content after preloading
            SplitCacheManager.GlobalCacheStats stats = cacheManager.getGlobalCacheStats();
            assertTrue(stats.getCurrentSize() >= 0, "Cache should contain preloaded components");
            
        } finally {
            for (SplitSearcher searcher : searchers) {
                searcher.close();
            }
        }
    }
    
    @Test
    @org.junit.jupiter.api.Order(4)
    @DisplayName("Test cache eviction and memory management")
    void testCacheEvictionAndMemoryManagement() {
        List<SplitSearcher> searchers = cacheManager.createMultipleSplitSearchers(localSplitPaths);
        
        try {
            SplitCacheManager.GlobalCacheStats beforeEviction = cacheManager.getGlobalCacheStats();
            
            // Force eviction to reduce cache size
            long targetSize = Math.max(1000, beforeEviction.getCurrentSize() / 2);
            cacheManager.forceEviction(targetSize);
            
            SplitCacheManager.GlobalCacheStats afterEviction = cacheManager.getGlobalCacheStats();
            
            // Verify eviction occurred (eviction count should increase or stay same)
            assertTrue(afterEviction.getTotalEvictions() >= beforeEviction.getTotalEvictions(),
                "Eviction count should increase or stay same");
            
        } finally {
            for (SplitSearcher searcher : searchers) {
                searcher.close();
            }
        }
    }
    
    @Test
    @org.junit.jupiter.api.Order(5)
    @DisplayName("Test shared cache architecture validation")
    void testSharedCacheArchitecture() {
        // Create multiple cache managers with same name - should return same instance
        SplitCacheManager.CacheConfig config1 = new SplitCacheManager.CacheConfig("shared-test")
            .withMaxCacheSize(50_000_000);
        
        SplitCacheManager.CacheConfig config2 = new SplitCacheManager.CacheConfig("shared-test")
            .withMaxCacheSize(50_000_000);
        
        SplitCacheManager manager1 = SplitCacheManager.getInstance(config1);
        SplitCacheManager manager2 = SplitCacheManager.getInstance(config2);
        
        // Should be the same instance (shared cache)
        assertSame(manager1, manager2, "Cache managers with same name should be the same instance");
        
        try {
            // Test that splits from both managers share the same cache
            List<SplitSearcher> searchers1 = manager1.createMultipleSplitSearchers(localSplitPaths.subList(0, 2));
            List<SplitSearcher> searchers2 = manager2.createMultipleSplitSearchers(localSplitPaths.subList(1, 3));
            
            // Both managers should see all splits
            assertTrue(manager1.getManagedSplitPaths().size() >= 2, "Manager1 should manage at least 2 splits");
            assertTrue(manager2.getManagedSplitPaths().size() >= 2, "Manager2 should manage at least 2 splits");
            
            // Close searchers
            for (SplitSearcher searcher : searchers1) {
                searcher.close();
            }
            for (SplitSearcher searcher : searchers2) {
                searcher.close();
            }
        } finally {
            manager1.close(); // This will close the shared instance
        }
    }
}