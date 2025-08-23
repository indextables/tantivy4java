package com.tantivy4java;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Global cache manager for Quickwit splits following Quickwit's multi-level caching strategy.
 * 
 * Implements Quickwit's proven caching architecture:
 * - LeafSearchCache: Global search result cache (per split_id + query)
 * - ByteRangeCache: Global storage byte range cache (per file_path + range)
 * - ComponentCache: Global component cache (fast fields, postings, etc.)
 * 
 * This matches Quickwit's design where caches are GLOBAL and shared across all splits,
 * not per-split as in our original flawed design.
 */
public class SplitCacheManager implements AutoCloseable {
    
    static {
        Tantivy.initialize();
    }
    
    private static final Map<String, SplitCacheManager> instances = new ConcurrentHashMap<>();
    
    private final String cacheName;
    private final long maxCacheSize;
    private final Map<String, SplitSearcher> managedSearchers;
    private final AtomicLong totalCacheSize;
    private final long nativePtr;
    
    /**
     * Configuration for shared cache across multiple splits
     */
    public static class CacheConfig {
        private final String cacheName;
        private long maxCacheSize = 200_000_000; // 200MB default for shared cache
        private int maxConcurrentLoads = 8;
        private boolean enableQueryCache = true;
        private Map<String, String> awsConfig = new HashMap<>();
        private Map<String, String> azureConfig = new HashMap<>();
        private Map<String, String> gcpConfig = new HashMap<>();
        
        public CacheConfig(String cacheName) {
            this.cacheName = cacheName;
        }
        
        public CacheConfig withMaxCacheSize(long maxCacheSize) {
            this.maxCacheSize = maxCacheSize;
            return this;
        }
        
        public CacheConfig withMaxConcurrentLoads(int maxConcurrentLoads) {
            this.maxConcurrentLoads = maxConcurrentLoads;
            return this;
        }
        
        public CacheConfig withQueryCache(boolean enabled) {
            this.enableQueryCache = enabled;
            return this;
        }
        
        public CacheConfig withAwsCredentials(String accessKey, String secretKey, String region) {
            this.awsConfig.put("access_key", accessKey);
            this.awsConfig.put("secret_key", secretKey);
            this.awsConfig.put("region", region);
            return this;
        }
        
        public CacheConfig withAwsEndpoint(String endpoint) {
            this.awsConfig.put("endpoint", endpoint);
            return this;
        }
        
        // Azure Blob Storage configuration
        public CacheConfig withAzureCredentials(String accountName, String accountKey) {
            this.azureConfig.put("account_name", accountName);
            this.azureConfig.put("account_key", accountKey);
            return this;
        }
        
        public CacheConfig withAzureConnectionString(String connectionString) {
            this.azureConfig.put("connection_string", connectionString);
            return this;
        }
        
        public CacheConfig withAzureEndpoint(String endpoint) {
            this.azureConfig.put("endpoint", endpoint);
            return this;
        }
        
        // GCP Cloud Storage configuration
        public CacheConfig withGcpCredentials(String projectId, String serviceAccountKey) {
            this.gcpConfig.put("project_id", projectId);
            this.gcpConfig.put("service_account_key", serviceAccountKey);
            return this;
        }
        
        public CacheConfig withGcpCredentialsFile(String credentialsFilePath) {
            this.gcpConfig.put("credentials_file", credentialsFilePath);
            return this;
        }
        
        public CacheConfig withGcpEndpoint(String endpoint) {
            this.gcpConfig.put("endpoint", endpoint);
            return this;
        }
        
        // Getters
        public String getCacheName() { return cacheName; }
        public long getMaxCacheSize() { return maxCacheSize; }
        public int getMaxConcurrentLoads() { return maxConcurrentLoads; }
        public boolean isQueryCacheEnabled() { return enableQueryCache; }
        public Map<String, String> getAwsConfig() { return awsConfig; }
        public Map<String, String> getAzureConfig() { return azureConfig; }
        public Map<String, String> getGcpConfig() { return gcpConfig; }
    }
    
    /**
     * Global cache statistics across all managed splits
     */
    public static class GlobalCacheStats {
        private final long totalHits;
        private final long totalMisses;
        private final long totalEvictions;
        private final long currentSize;
        private final long maxSize;
        private final int activeSplits;
        
        public GlobalCacheStats(long totalHits, long totalMisses, long totalEvictions, 
                               long currentSize, long maxSize, int activeSplits) {
            this.totalHits = totalHits;
            this.totalMisses = totalMisses;
            this.totalEvictions = totalEvictions;
            this.currentSize = currentSize;
            this.maxSize = maxSize;
            this.activeSplits = activeSplits;
        }
        
        public long getTotalHits() { return totalHits; }
        public long getTotalMisses() { return totalMisses; }
        public long getTotalEvictions() { return totalEvictions; }
        public long getCurrentSize() { return currentSize; }
        public long getMaxSize() { return maxSize; }
        public int getActiveSplits() { return activeSplits; }
        public double getHitRate() { 
            long total = totalHits + totalMisses;
            return total > 0 ? (double) totalHits / total : 0.0; 
        }
        
        public double getUtilization() {
            return maxSize > 0 ? (double) currentSize / maxSize : 0.0;
        }
    }
    
    private SplitCacheManager(CacheConfig config) {
        this.cacheName = config.getCacheName();
        this.maxCacheSize = config.getMaxCacheSize();
        this.managedSearchers = new ConcurrentHashMap<>();
        this.totalCacheSize = new AtomicLong(0);
        this.nativePtr = createNativeCacheManager(config);
    }
    
    /**
     * Get or create a shared cache manager instance
     */
    public static SplitCacheManager getInstance(CacheConfig config) {
        return instances.computeIfAbsent(config.getCacheName(), 
            name -> new SplitCacheManager(config));
    }
    
    /**
     * Create a split searcher that uses this shared cache
     */
    public SplitSearcher createSplitSearcher(String splitPath) {
        return createSplitSearcher(splitPath, new HashMap<>());
    }
    
    /**
     * Create a split searcher with custom configuration that uses this shared cache
     */
    public SplitSearcher createSplitSearcher(String splitPath, Map<String, Object> splitConfig) {
        SplitSearcher searcher = new SplitSearcher(splitPath, this, splitConfig);
        managedSearchers.put(splitPath, searcher);
        return searcher;
    }
    
    /**
     * Create multiple split searchers sharing the same cache (batch creation)
     */
    public List<SplitSearcher> createMultipleSplitSearchers(List<String> splitPaths) {
        List<SplitSearcher> searchers = new ArrayList<>();
        for (String splitPath : splitPaths) {
            searchers.add(createSplitSearcher(splitPath));
        }
        return searchers;
    }
    
    /**
     * Multi-split search across all managed splits
     * Implements distributed search pattern similar to Quickwit's merge_search
     */
    public SearchResult searchAcrossAllSplits(Query query, int totalLimit) {
        return searchAcrossAllSplitsNative(nativePtr, query.getNativePtr(), totalLimit);
    }
    
    /**
     * Multi-split search across specific splits
     */
    public SearchResult searchAcrossSplits(List<String> splitPaths, Query query, int totalLimit) {
        return searchAcrossSplitsNative(nativePtr, splitPaths, query.getNativePtr(), totalLimit);
    }
    
    /**
     * Remove a split searcher from management
     */
    void removeSplitSearcher(String splitPath) {
        managedSearchers.remove(splitPath);
    }
    
    /**
     * Get all currently managed split paths
     */
    public Set<String> getManagedSplitPaths() {
        return new HashSet<>(managedSearchers.keySet());
    }
    
    /**
     * Get global cache statistics across all managed splits
     */
    public GlobalCacheStats getGlobalCacheStats() {
        // Get native stats but pass the correct active splits count from Java
        GlobalCacheStats nativeStats = getGlobalCacheStatsNative(nativePtr);
        
        // Override the active splits with the correct Java-managed count
        return new GlobalCacheStats(
            nativeStats.getTotalHits(),
            nativeStats.getTotalMisses(), 
            nativeStats.getTotalEvictions(),
            nativeStats.getCurrentSize(),
            nativeStats.getMaxSize(),
            managedSearchers.size() // Use Java-managed count
        );
    }
    
    /**
     * Preload components for a specific split
     */
    public void preloadComponents(String splitPath, Set<SplitSearcher.IndexComponent> components) {
        preloadComponentsNative(nativePtr, splitPath, components);
    }
    
    /**
     * Evict components for a specific split or globally
     */
    public void evictComponents(String splitPath, Set<SplitSearcher.IndexComponent> components) {
        evictComponentsNative(nativePtr, splitPath, components);
    }
    
    /**
     * Force eviction to free up memory
     */
    public void forceEviction(long targetSizeBytes) {
        forceEvictionNative(nativePtr, targetSizeBytes);
    }
    
    @Override
    public void close() {
        // Close all managed searchers
        for (SplitSearcher searcher : managedSearchers.values()) {
            try {
                searcher.close();
            } catch (Exception e) {
                // Log error but continue cleanup
            }
        }
        managedSearchers.clear();
        
        // Close native cache manager
        if (nativePtr != 0) {
            closeNativeCacheManager(nativePtr);
        }
        
        // Remove from instances
        instances.remove(cacheName);
    }
    
    // Native method declarations
    private static native long createNativeCacheManager(CacheConfig config);
    private static native void closeNativeCacheManager(long ptr);
    private static native GlobalCacheStats getGlobalCacheStatsNative(long ptr);
    private static native void preloadComponentsNative(long ptr, String splitPath, Set<SplitSearcher.IndexComponent> components);
    private static native void evictComponentsNative(long ptr, String splitPath, Set<SplitSearcher.IndexComponent> components);
    private static native void forceEvictionNative(long ptr, long targetSizeBytes);
    private static native SearchResult searchAcrossAllSplitsNative(long ptr, long queryPtr, int totalLimit);
    private static native SearchResult searchAcrossSplitsNative(long ptr, List<String> splitPaths, long queryPtr, int totalLimit);
    
    // Package-private getters for SplitSearcher
    String getCacheName() { return cacheName; }
    long getMaxCacheSize() { return maxCacheSize; }
    long getNativePtr() { return nativePtr; }
}