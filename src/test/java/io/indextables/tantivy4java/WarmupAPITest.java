package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.split.*;


import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple test to verify the warmup API exists and is accessible.
 * This test validates the API surface without executing potentially hanging async operations.
 */
public class WarmupAPITest {

    @Test
    @DisplayName("Verify warmup methods exist in SplitSearcher")
    void testWarmupMethodsExist() {
        // Test that the warmup-related classes and enums exist
        assertNotNull(SplitSearcher.IndexComponent.SCHEMA);
        assertNotNull(SplitSearcher.IndexComponent.STORE);
        assertNotNull(SplitSearcher.IndexComponent.FASTFIELD);
        assertNotNull(SplitSearcher.IndexComponent.POSTINGS);
        assertNotNull(SplitSearcher.IndexComponent.POSITIONS);
        assertNotNull(SplitSearcher.IndexComponent.FIELDNORM);
        assertNotNull(SplitSearcher.IndexComponent.TERM);

        // Verify IndexComponent enum values
        SplitSearcher.IndexComponent[] components = SplitSearcher.IndexComponent.values();
        assertEquals(7, components.length, "Should have 7 index components");
    }

    @Test
    @DisplayName("Verify WarmupStats class structure")
    void testWarmupStatsClass() {
        // Create a test WarmupStats object
        java.util.Map<SplitSearcher.IndexComponent, Long> componentSizes = new java.util.HashMap<>();
        componentSizes.put(SplitSearcher.IndexComponent.FASTFIELD, 1000L);
        
        SplitSearcher.WarmupStats stats = new SplitSearcher.WarmupStats(
            10000L,  // totalBytesLoaded
            100L,    // warmupTimeMs
            2,       // componentsLoaded
            componentSizes,
            true     // usedParallelLoading
        );
        
        // Verify getters work
        assertEquals(10000L, stats.getTotalBytesLoaded());
        assertEquals(100L, stats.getWarmupTimeMs());
        assertEquals(2, stats.getComponentsLoaded());
        assertTrue(stats.isUsedParallelLoading());
        assertNotNull(stats.getComponentSizes());
        assertEquals(1, stats.getComponentSizes().size());
        
        // Test loading speed calculation
        double speed = stats.getLoadingSpeedMBps();
        assertTrue(speed > 0, "Loading speed should be positive");
    }

    @Test
    @DisplayName("Verify CacheStats class structure")
    void testCacheStatsClass() {
        // Create a test CacheStats object
        SplitSearcher.CacheStats cacheStats = new SplitSearcher.CacheStats(
            100L,     // hitCount
            50L,      // missCount
            10L,      // evictionCount
            500000L,  // totalSize
            1000000L  // maxSize
        );
        
        // Verify getters work
        assertEquals(100L, cacheStats.getHitCount());
        assertEquals(50L, cacheStats.getMissCount());
        assertEquals(10L, cacheStats.getEvictionCount());
        assertEquals(500000L, cacheStats.getTotalSize());
        assertEquals(1000000L, cacheStats.getMaxSize());
        assertEquals(150L, cacheStats.getTotalRequests());
        
        // Test calculated values
        double hitRate = cacheStats.getHitRate();
        assertEquals(100.0 / 150.0, hitRate, 0.001, "Hit rate should be hits/total");
        
        double utilization = cacheStats.getUtilization();
        assertEquals(0.5, utilization, 0.001, "Utilization should be 50%");
    }

    @Test
    @DisplayName("Verify LoadingStats and ComponentStats classes")
    void testLoadingStatsClasses() {
        // Create ComponentStats
        SplitSearcher.ComponentStats compStats = new SplitSearcher.ComponentStats(
            5000L,  // bytesLoaded
            50L,    // loadTime
            5,      // loadCount
            true    // isPreloaded
        );
        
        assertEquals(5000L, compStats.getBytesLoaded());
        assertEquals(50L, compStats.getLoadTime());
        assertEquals(5, compStats.getLoadCount());
        assertTrue(compStats.isPreloaded());
        assertEquals(10.0, compStats.getAverageLoadTime(), 0.001);
        
        // Create LoadingStats
        java.util.Map<SplitSearcher.IndexComponent, SplitSearcher.ComponentStats> componentStats = new java.util.HashMap<>();
        componentStats.put(SplitSearcher.IndexComponent.FASTFIELD, compStats);
        
        SplitSearcher.LoadingStats loadingStats = new SplitSearcher.LoadingStats(
            10000L,  // totalBytesLoaded
            100L,    // totalLoadTime
            2,       // activeConcurrentLoads
            componentStats
        );
        
        assertEquals(10000L, loadingStats.getTotalBytesLoaded());
        assertEquals(100L, loadingStats.getTotalLoadTime());
        assertEquals(2, loadingStats.getActiveConcurrentLoads());
        assertNotNull(loadingStats.getComponentStats());
        assertEquals(1, loadingStats.getComponentStats().size());
        
        double avgSpeed = loadingStats.getAverageLoadSpeed();
        assertTrue(avgSpeed > 0, "Average load speed should be positive");
    }

    @Test
    @DisplayName("Verify SplitMetadata class structure")
    void testSplitMetadataClass() {
        java.util.Map<String, Long> componentSizes = new java.util.HashMap<>();
        componentSizes.put("postings", 1000L);
        componentSizes.put("store", 2000L);
        
        SplitSearcher.SplitMetadata metadata = new SplitSearcher.SplitMetadata(
            "split-123",  // splitId
            10000L,       // totalSize
            3000L,        // hotCacheSize
            2,            // numComponents
            componentSizes
        );
        
        assertEquals("split-123", metadata.getSplitId());
        assertEquals(10000L, metadata.getTotalSize());
        assertEquals(3000L, metadata.getHotCacheSize());
        assertEquals(2, metadata.getNumComponents());
        assertNotNull(metadata.getComponentSizes());
        assertEquals(2, metadata.getComponentSizes().size());
        assertEquals(0.3, metadata.getHotCacheRatio(), 0.001);
    }
}
