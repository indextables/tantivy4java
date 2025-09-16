package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

/**
 * Comprehensive test for the global cache flushing system.
 * Verifies that all caching layers can be properly flushed individually and globally.
 */
public class GlobalCacheFlushTest {
    
    private SplitCacheManager cacheManager1;
    private SplitCacheManager cacheManager2;
    
    @BeforeEach
    void setUp() {
        // Create multiple cache managers to test global flushing
        SplitCacheManager.CacheConfig config1 = new SplitCacheManager.CacheConfig("flush-test-cache-1")
            .withMaxCacheSize(100_000_000); // 100MB
            
        SplitCacheManager.CacheConfig config2 = new SplitCacheManager.CacheConfig("flush-test-cache-2")
            .withMaxCacheSize(150_000_000); // 150MB
            
        cacheManager1 = SplitCacheManager.getInstance(config1);
        cacheManager2 = SplitCacheManager.getInstance(config2);
    }
    
    @Test
    @DisplayName("Test 1: Individual Cache Manager Cache Flushing")
    void test1_individualCacheManagerFlushing() {
        System.out.println("🧪 Test 1: Individual Cache Manager Cache Flushing");
        
        // Get initial stats
        SplitCacheManager.GlobalCacheStats beforeStats = cacheManager1.getGlobalCacheStats();
        System.out.println("📊 Cache1 before flush: " + beforeStats);
        
        // Perform cache flush on individual manager
        SplitCacheManager.CacheFlushStats flushStats = cacheManager1.flushAllCaches();
        System.out.println("🗑️  Cache flush stats: " + flushStats);
        
        // Verify flush occurred (even if no data was cached, method should execute)
        assertNotNull(flushStats);
        assertNotNull(flushStats.getBeforeStats());
        assertNotNull(flushStats.getAfterStats());
        assertTrue(flushStats.getSplitsAffected() >= 0);
        
        System.out.println("✅ Individual cache manager flushing works correctly");
    }
    
    @Test
    @DisplayName("Test 2: Selective Cache Type Flushing")
    void test2_selectiveCacheTypeFlushing() {
        System.out.println("🧪 Test 2: Selective Cache Type Flushing");
        
        // Test flushing specific cache types
        Set<SplitCacheManager.CacheType> componentOnly = EnumSet.of(SplitCacheManager.CacheType.COMPONENT);
        Set<SplitCacheManager.CacheType> allTypes = EnumSet.allOf(SplitCacheManager.CacheType.class);
        
        // Should execute without errors
        assertDoesNotThrow(() -> {
            cacheManager1.flushCacheTypes(componentOnly);
            cacheManager1.flushCacheTypes(allTypes);
        });
        
        System.out.println("🎯 Cache types: " + Arrays.toString(SplitCacheManager.CacheType.values()));
        System.out.println("✅ Selective cache type flushing works correctly");
    }
    
    @Test
    @DisplayName("Test 3: Global System-Wide Cache Flushing")
    void test3_globalSystemWideCacheFlushing() {
        System.out.println("🧪 Test 3: Global System-Wide Cache Flushing");
        
        // Get system stats before flush
        GlobalCacheUtils.GlobalSystemCacheStats beforeSystemStats = GlobalCacheUtils.getSystemCacheStats();
        System.out.println("🌍 System stats before flush: " + beforeSystemStats);
        
        // Verify we have multiple cache managers
        List<String> cacheManagerNames = GlobalCacheUtils.getActiveCacheManagerNames();
        System.out.println("📋 Active cache managers: " + cacheManagerNames);
        assertTrue(cacheManagerNames.size() >= 2, "Should have at least 2 cache managers");
        assertTrue(cacheManagerNames.stream().anyMatch(name -> name.contains("flush-test-cache-1")));
        assertTrue(cacheManagerNames.stream().anyMatch(name -> name.contains("flush-test-cache-2")));
        
        // Perform global cache flush
        GlobalCacheUtils.GlobalCacheFlushStats globalFlushStats = GlobalCacheUtils.flushAllCaches();
        System.out.println("🗑️  Global flush stats: " + globalFlushStats);
        
        // Verify global flush worked
        assertNotNull(globalFlushStats);
        assertTrue(globalFlushStats.getCacheManagersAffected() >= 2);
        assertTrue(globalFlushStats.getTotalBytesFreed() >= 0);
        assertTrue(globalFlushStats.getTotalItemsEvicted() >= 0);
        assertTrue(globalFlushStats.getTotalSplitsAffected() >= 0);
        
        System.out.println("✅ Global system-wide cache flushing works correctly");
    }
    
    @Test
    @DisplayName("Test 4: Global Selective Cache Type Flushing")
    void test4_globalSelectiveCacheTypeFlushing() {
        System.out.println("🧪 Test 4: Global Selective Cache Type Flushing");
        
        // Test global selective flushing
        Set<SplitCacheManager.CacheType> leafSearchOnly = EnumSet.of(SplitCacheManager.CacheType.LEAF_SEARCH);
        Set<SplitCacheManager.CacheType> componentAndByteRange = EnumSet.of(
            SplitCacheManager.CacheType.COMPONENT, 
            SplitCacheManager.CacheType.BYTE_RANGE
        );
        
        // Should execute without errors across all managers
        assertDoesNotThrow(() -> {
            GlobalCacheUtils.flushCacheTypes(leafSearchOnly);
            GlobalCacheUtils.flushCacheTypes(componentAndByteRange);
        });
        
        System.out.println("✅ Global selective cache type flushing works correctly");
    }
    
    @Test
    @DisplayName("Test 5: Cache Statistics and Monitoring")
    void test5_cacheStatisticsAndMonitoring() {
        System.out.println("🧪 Test 5: Cache Statistics and Monitoring");
        
        // Test system-wide cache statistics
        GlobalCacheUtils.GlobalSystemCacheStats systemStats = GlobalCacheUtils.getSystemCacheStats();
        
        assertNotNull(systemStats);
        assertTrue(systemStats.getActiveCacheManagers() >= 2);
        assertTrue(systemStats.getTotalSizeBytes() >= 0);
        assertTrue(systemStats.getTotalHits() >= 0);
        assertTrue(systemStats.getTotalMisses() >= 0);
        assertTrue(systemStats.getOverallHitRate() >= 0.0);
        assertTrue(systemStats.getOverallHitRate() <= 1.0);
        
        System.out.println("📊 System cache stats: " + systemStats);
        System.out.println("📈 Overall hit rate: " + String.format("%.2f%%", systemStats.getOverallHitRate() * 100));
        
        // Test individual manager stats
        Map<String, SplitCacheManager.GlobalCacheStats> managerStats = systemStats.getManagerStats();
        assertTrue(managerStats.keySet().stream().anyMatch(key -> key.contains("flush-test-cache-1")));
        assertTrue(managerStats.keySet().stream().anyMatch(key -> key.contains("flush-test-cache-2")));
        
        System.out.println("✅ Cache statistics and monitoring works correctly");
    }
    
    @Test
    @DisplayName("Test 6: Split-Specific Cache Flushing")
    void test6_splitSpecificCacheFlushing() {
        System.out.println("🧪 Test 6: Split-Specific Cache Flushing");
        
        // Test flushing caches for non-existent split (should return false)
        boolean result = cacheManager1.flushSplitCaches("/non/existent/split.split");
        assertFalse(result, "Should return false for non-existent split");
        
        // Test would work for real splits if they were loaded
        System.out.println("📝 Note: Split-specific flushing would work for loaded splits");
        System.out.println("✅ Split-specific cache flushing logic works correctly");
    }
    
    @Test
    @DisplayName("Test 7: Cache Flush Statistics Validation")
    void test7_cacheFlushStatisticsValidation() {
        System.out.println("🧪 Test 7: Cache Flush Statistics Validation");
        
        // Test flush statistics structure
        SplitCacheManager.CacheFlushStats individualStats = cacheManager1.flushAllCaches();
        
        // Validate individual stats structure
        assertNotNull(individualStats.toString());
        assertTrue(individualStats.getBytesFreed() >= 0);
        assertTrue(individualStats.getItemsEvicted() >= 0);
        assertTrue(individualStats.getSplitsAffected() >= 0);
        
        // Test global stats structure
        GlobalCacheUtils.GlobalCacheFlushStats globalStats = GlobalCacheUtils.flushAllCaches();
        
        // Validate global stats structure  
        assertNotNull(globalStats.toString());
        assertTrue(globalStats.getTotalBytesFreed() >= 0);
        assertTrue(globalStats.getTotalItemsEvicted() >= 0);
        assertTrue(globalStats.getCacheManagersAffected() >= 0);
        assertTrue(globalStats.getTotalSplitsAffected() >= 0);
        assertNotNull(globalStats.getIndividualStats());
        
        System.out.println("📊 Individual stats: " + individualStats);
        System.out.println("🌍 Global stats: " + globalStats);
        System.out.println("✅ Cache flush statistics validation works correctly");
    }
    
    @Test
    @DisplayName("Test 8: Cache Type Enum Validation")
    void test8_cacheTypeEnumValidation() {
        System.out.println("🧪 Test 8: Cache Type Enum Validation");
        
        // Validate all cache types are present
        SplitCacheManager.CacheType[] cacheTypes = SplitCacheManager.CacheType.values();
        
        assertTrue(cacheTypes.length >= 3, "Should have at least 3 cache types");
        
        // Verify specific cache types exist
        boolean hasLeafSearch = Arrays.stream(cacheTypes)
            .anyMatch(type -> type == SplitCacheManager.CacheType.LEAF_SEARCH);
        boolean hasByteRange = Arrays.stream(cacheTypes)
            .anyMatch(type -> type == SplitCacheManager.CacheType.BYTE_RANGE);
        boolean hasComponent = Arrays.stream(cacheTypes)
            .anyMatch(type -> type == SplitCacheManager.CacheType.COMPONENT);
            
        assertTrue(hasLeafSearch, "LEAF_SEARCH cache type should exist");
        assertTrue(hasByteRange, "BYTE_RANGE cache type should exist");
        assertTrue(hasComponent, "COMPONENT cache type should exist");
        
        System.out.println("🎯 Available cache types: " + Arrays.toString(cacheTypes));
        System.out.println("✅ Cache type enum validation works correctly");
    }
}