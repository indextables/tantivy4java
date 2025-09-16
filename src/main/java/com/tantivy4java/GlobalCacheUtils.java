package com.tantivy4java;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Global cache utilities for comprehensive cache management across all SplitCacheManager instances.
 * 
 * <h3>Purpose</h3>
 * Provides system-wide cache flushing capabilities that work across all active SplitCacheManager
 * instances, including ones that might be held by different parts of an application.
 * 
 * <h3>Usage Examples</h3>
 * <pre>{@code
 * // Flush ALL caches in the entire system
 * GlobalCacheFlushStats stats = GlobalCacheUtils.flushAllCaches();
 * System.out.println("Freed " + stats.getTotalBytesFreed() + " bytes across " + 
 *                    stats.getCacheManagersAffected() + " cache managers");
 * 
 * // Flush only specific cache types
 * Set<SplitCacheManager.CacheType> cacheTypes = EnumSet.of(
 *     SplitCacheManager.CacheType.COMPONENT, 
 *     SplitCacheManager.CacheType.LEAF_SEARCH
 * );
 * GlobalCacheUtils.flushCacheTypes(cacheTypes);
 * 
 * // Get comprehensive cache statistics across all managers
 * GlobalSystemCacheStats systemStats = GlobalCacheUtils.getSystemCacheStats();
 * System.out.println("Total system cache usage: " + systemStats.getTotalSizeBytes() + " bytes");
 * </pre>
 * 
 * <h3>Thread Safety</h3>
 * All methods in this class are thread-safe and can be called concurrently from multiple threads.
 */
public class GlobalCacheUtils {
    
    /**
     * Flush ALL caches across ALL SplitCacheManager instances in the entire JVM.
     * This is the most comprehensive cache clearing operation available.
     * 
     * @return GlobalCacheFlushStats containing detailed information about what was flushed
     */
    public static GlobalCacheFlushStats flushAllCaches() {
        Map<String, SplitCacheManager> allInstances = SplitCacheManager.getAllInstances();
        List<SplitCacheManager.CacheFlushStats> individualStats = new ArrayList<>();
        
        for (SplitCacheManager manager : allInstances.values()) {
            try {
                SplitCacheManager.CacheFlushStats stats = manager.flushAllCaches();
                individualStats.add(stats);
            } catch (Exception e) {
                // Log error but continue with other managers
                System.err.println("Warning: Failed to flush cache manager '" + 
                                 manager.getCacheName() + "': " + e.getMessage());
            }
        }
        
        return new GlobalCacheFlushStats(individualStats);
    }
    
    /**
     * Flush specific cache types across ALL SplitCacheManager instances.
     * 
     * @param cacheTypes Set of cache types to flush
     */
    public static void flushCacheTypes(Set<SplitCacheManager.CacheType> cacheTypes) {
        Map<String, SplitCacheManager> allInstances = SplitCacheManager.getAllInstances();
        
        for (SplitCacheManager manager : allInstances.values()) {
            try {
                manager.flushCacheTypes(cacheTypes);
            } catch (Exception e) {
                // Log error but continue with other managers
                System.err.println("Warning: Failed to flush cache types for manager '" + 
                                 manager.getCacheName() + "': " + e.getMessage());
            }
        }
    }
    
    /**
     * Get comprehensive cache statistics across the entire system.
     * 
     * @return GlobalSystemCacheStats containing aggregated statistics from all cache managers
     */
    public static GlobalSystemCacheStats getSystemCacheStats() {
        Map<String, SplitCacheManager> allInstances = SplitCacheManager.getAllInstances();
        Map<String, SplitCacheManager.GlobalCacheStats> managerStats = new HashMap<>();
        
        for (Map.Entry<String, SplitCacheManager> entry : allInstances.entrySet()) {
            try {
                managerStats.put(entry.getKey(), entry.getValue().getGlobalCacheStats());
            } catch (Exception e) {
                // Log error but continue with other managers
                System.err.println("Warning: Failed to get stats for cache manager '" + 
                                 entry.getKey() + "': " + e.getMessage());
            }
        }
        
        return new GlobalSystemCacheStats(managerStats);
    }
    
    /**
     * Get a list of all active SplitCacheManager names in the system.
     * 
     * @return List of cache manager names
     */
    public static List<String> getActiveCacheManagerNames() {
        return new ArrayList<>(SplitCacheManager.getAllInstances().keySet());
    }
    
    /**
     * Statistics about a global cache flush operation across multiple SplitCacheManager instances.
     */
    public static class GlobalCacheFlushStats {
        private final List<SplitCacheManager.CacheFlushStats> individualStats;
        private final long totalBytesFreed;
        private final long totalItemsEvicted;
        private final int cacheManagersAffected;
        private final int totalSplitsAffected;
        
        public GlobalCacheFlushStats(List<SplitCacheManager.CacheFlushStats> individualStats) {
            this.individualStats = new ArrayList<>(individualStats);
            this.cacheManagersAffected = individualStats.size();
            this.totalBytesFreed = individualStats.stream().mapToLong(SplitCacheManager.CacheFlushStats::getBytesFreed).sum();
            this.totalItemsEvicted = individualStats.stream().mapToLong(SplitCacheManager.CacheFlushStats::getItemsEvicted).sum();
            this.totalSplitsAffected = individualStats.stream().mapToInt(SplitCacheManager.CacheFlushStats::getSplitsAffected).sum();
        }
        
        public long getTotalBytesFreed() { return totalBytesFreed; }
        public long getTotalItemsEvicted() { return totalItemsEvicted; }
        public int getCacheManagersAffected() { return cacheManagersAffected; }
        public int getTotalSplitsAffected() { return totalSplitsAffected; }
        public List<SplitCacheManager.CacheFlushStats> getIndividualStats() { return new ArrayList<>(individualStats); }
        
        @Override
        public String toString() {
            return String.format("GlobalCacheFlushStats{totalBytesFreed=%d, totalItemsEvicted=%d, " +
                               "cacheManagersAffected=%d, totalSplitsAffected=%d}", 
                               totalBytesFreed, totalItemsEvicted, cacheManagersAffected, totalSplitsAffected);
        }
    }
    
    /**
     * System-wide cache statistics aggregated across all SplitCacheManager instances.
     */
    public static class GlobalSystemCacheStats {
        private final Map<String, SplitCacheManager.GlobalCacheStats> managerStats;
        private final long totalSizeBytes;
        private final long totalHits;
        private final long totalMisses;
        private final int activeCacheManagers;
        
        public GlobalSystemCacheStats(Map<String, SplitCacheManager.GlobalCacheStats> managerStats) {
            this.managerStats = new HashMap<>(managerStats);
            this.activeCacheManagers = managerStats.size();
            this.totalSizeBytes = managerStats.values().stream()
                .mapToLong(SplitCacheManager.GlobalCacheStats::getCurrentSize).sum();
            this.totalHits = managerStats.values().stream()
                .mapToLong(SplitCacheManager.GlobalCacheStats::getTotalHits).sum();
            this.totalMisses = managerStats.values().stream()
                .mapToLong(SplitCacheManager.GlobalCacheStats::getTotalMisses).sum();
        }
        
        public long getTotalSizeBytes() { return totalSizeBytes; }
        public long getTotalHits() { return totalHits; }
        public long getTotalMisses() { return totalMisses; }
        public int getActiveCacheManagers() { return activeCacheManagers; }
        public double getOverallHitRate() { 
            long totalRequests = totalHits + totalMisses;
            return totalRequests > 0 ? (double) totalHits / totalRequests : 0.0;
        }
        
        public Map<String, SplitCacheManager.GlobalCacheStats> getManagerStats() { 
            return new HashMap<>(managerStats); 
        }
        
        @Override
        public String toString() {
            return String.format("GlobalSystemCacheStats{totalSizeBytes=%d, totalHits=%d, totalMisses=%d, " +
                               "hitRate=%.2f%%, activeCacheManagers=%d}", 
                               totalSizeBytes, totalHits, totalMisses, getOverallHitRate() * 100, activeCacheManagers);
        }
    }
}