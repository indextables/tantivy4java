package com.tantivy4java;

import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * SplitSearcher provides efficient search capabilities over Quickwit split files
 * with hot cache optimization and incremental loading of index components.
 */
public class SplitSearcher implements AutoCloseable {
    
    static {
        Tantivy.initialize();
    }

    private long nativePtr;
    private final String splitPath;
    private final SplitSearchConfig config;
    
    /**
     * Configuration options for split searching with hot cache optimization
     */
    public static class SplitSearchConfig {
        private final String splitPath;
        private long cacheSize = 50_000_000; // 50MB default
        private Set<IndexComponent> preloadComponents = EnumSet.noneOf(IndexComponent.class);
        private Duration maxWaitTime = Duration.ofSeconds(30);
        private int maxConcurrentLoads = 4;
        private boolean enableQueryCache = true;
        private Map<String, String> awsConfig = new HashMap<>();
        
        public SplitSearchConfig(String splitPath) {
            this.splitPath = splitPath;
        }
        
        public SplitSearchConfig withCacheSize(long cacheSize) {
            this.cacheSize = cacheSize;
            return this;
        }
        
        public SplitSearchConfig preload(IndexComponent... components) {
            this.preloadComponents.addAll(Arrays.asList(components));
            return this;
        }
        
        public SplitSearchConfig withMaxWaitTime(Duration duration) {
            this.maxWaitTime = duration;
            return this;
        }
        
        public SplitSearchConfig withMaxConcurrentLoads(int max) {
            this.maxConcurrentLoads = max;
            return this;
        }
        
        public SplitSearchConfig enableQueryCache(boolean enable) {
            this.enableQueryCache = enable;
            return this;
        }
        
        public SplitSearchConfig withAwsCredentials(String accessKey, String secretKey, String region) {
            this.awsConfig.put("access_key", accessKey);
            this.awsConfig.put("secret_key", secretKey);
            this.awsConfig.put("region", region);
            return this;
        }
        
        public SplitSearchConfig withAwsEndpoint(String endpoint) {
            this.awsConfig.put("endpoint", endpoint);
            return this;
        }
        
        // Getters
        public String getSplitPath() { return splitPath; }
        public long getCacheSize() { return cacheSize; }
        public Set<IndexComponent> getPreloadComponents() { return preloadComponents; }
        public Duration getMaxWaitTime() { return maxWaitTime; }
        public int getMaxConcurrentLoads() { return maxConcurrentLoads; }
        public boolean isQueryCacheEnabled() { return enableQueryCache; }
        public Map<String, String> getAwsConfig() { return awsConfig; }
    }
    
    /**
     * Index components that can be preloaded for optimal performance
     */
    public enum IndexComponent {
        SCHEMA,      // Schema and field metadata
        STORE,       // Document storage
        FASTFIELD,   // Fast field data for sorting/filtering
        POSTINGS,    // Term postings lists
        POSITIONS,   // Term positions for phrase queries
        FIELDNORM    // Field norm data for scoring
    }
    
    /**
     * Statistics about cache performance and loading behavior
     */
    public static class CacheStats {
        private final long hitCount;
        private final long missCount;
        private final long evictionCount;
        private final long totalSize;
        private final long maxSize;
        
        public CacheStats(long hitCount, long missCount, long evictionCount, long totalSize, long maxSize) {
            this.hitCount = hitCount;
            this.missCount = missCount;
            this.evictionCount = evictionCount;
            this.totalSize = totalSize;
            this.maxSize = maxSize;
        }
        
        public double getHitRate() { 
            long total = hitCount + missCount;
            return total > 0 ? (double) hitCount / total : 0.0;
        }
        
        public long getHitCount() { return hitCount; }
        public long getMissCount() { return missCount; }
        public long getEvictionCount() { return evictionCount; }
        public long getTotalSize() { return totalSize; }
        public long getMaxSize() { return maxSize; }
        public long getTotalRequests() { return hitCount + missCount; }
        public double getUtilization() { return (double) totalSize / maxSize; }
    }
    
    /**
     * Statistics about component loading performance
     */
    public static class LoadingStats {
        private final long totalBytesLoaded;
        private final long totalLoadTime;
        private final int activeConcurrentLoads;
        private final Map<IndexComponent, ComponentStats> componentStats;
        
        public LoadingStats(long totalBytesLoaded, long totalLoadTime, int activeConcurrentLoads, 
                           Map<IndexComponent, ComponentStats> componentStats) {
            this.totalBytesLoaded = totalBytesLoaded;
            this.totalLoadTime = totalLoadTime;
            this.activeConcurrentLoads = activeConcurrentLoads;
            this.componentStats = new HashMap<>(componentStats);
        }
        
        public long getTotalBytesLoaded() { return totalBytesLoaded; }
        public long getTotalLoadTime() { return totalLoadTime; }
        public int getActiveConcurrentLoads() { return activeConcurrentLoads; }
        public Map<IndexComponent, ComponentStats> getComponentStats() { return componentStats; }
        public double getAverageLoadSpeed() { 
            return totalLoadTime > 0 ? (double) totalBytesLoaded / totalLoadTime * 1000 : 0; // bytes per second
        }
    }
    
    /**
     * Per-component loading statistics
     */
    public static class ComponentStats {
        private final long bytesLoaded;
        private final long loadTime;
        private final int loadCount;
        private final boolean isPreloaded;
        
        public ComponentStats(long bytesLoaded, long loadTime, int loadCount, boolean isPreloaded) {
            this.bytesLoaded = bytesLoaded;
            this.loadTime = loadTime;
            this.loadCount = loadCount;
            this.isPreloaded = isPreloaded;
        }
        
        public long getBytesLoaded() { return bytesLoaded; }
        public long getLoadTime() { return loadTime; }
        public int getLoadCount() { return loadCount; }
        public boolean isPreloaded() { return isPreloaded; }
        public double getAverageLoadTime() { return loadCount > 0 ? (double) loadTime / loadCount : 0; }
    }
    
    private SplitSearcher(String splitPath, SplitSearchConfig config, long nativePtr) {
        this.splitPath = splitPath;
        this.config = config;
        this.nativePtr = nativePtr;
    }
    
    /**
     * Create a new SplitSearcher from a split file
     */
    public static SplitSearcher create(SplitSearchConfig config) {
        long nativePtr = createNative(config);
        return new SplitSearcher(config.getSplitPath(), config, nativePtr);
    }
    
    /**
     * Builder pattern for creating SplitSearcher instances
     */
    public static Builder builder(String splitPath) {
        return new Builder(splitPath);
    }
    
    public static class Builder {
        private final SplitSearchConfig config;
        
        private Builder(String splitPath) {
            this.config = new SplitSearchConfig(splitPath);
        }
        
        public Builder withCacheSize(long cacheSize) {
            config.withCacheSize(cacheSize);
            return this;
        }
        
        public Builder preload(IndexComponent... components) {
            config.preload(components);
            return this;
        }
        
        public Builder withMaxWaitTime(Duration duration) {
            config.withMaxWaitTime(duration);
            return this;
        }
        
        public Builder withMaxConcurrentLoads(int max) {
            config.withMaxConcurrentLoads(max);
            return this;
        }
        
        public Builder enableQueryCache(boolean enable) {
            config.enableQueryCache(enable);
            return this;
        }
        
        public Builder withAwsCredentials(String accessKey, String secretKey, String region) {
            config.withAwsCredentials(accessKey, secretKey, region);
            return this;
        }
        
        public Builder withAwsEndpoint(String endpoint) {
            config.withAwsEndpoint(endpoint);
            return this;
        }
        
        public SplitSearcher build() {
            return SplitSearcher.create(config);
        }
    }
    
    /**
     * Get the schema for this split
     */
    public Schema getSchema() {
        return new Schema(getSchemaFromNative(nativePtr));
    }
    
    /**
     * Search the split with hot cache optimization
     */
    public SearchResult search(Query query, int limit) {
        return searchNative(nativePtr, query.getNativePtr(), limit);
    }
    
    /**
     * Asynchronously preload specified components into cache
     */
    public CompletableFuture<Void> preloadComponents(IndexComponent... components) {
        return CompletableFuture.runAsync(() -> {
            preloadComponentsNative(nativePtr, components);
        });
    }
    
    /**
     * Get current cache statistics
     */
    public CacheStats getCacheStats() {
        return getCacheStatsNative(nativePtr);
    }
    
    /**
     * Get current loading statistics
     */
    public LoadingStats getLoadingStats() {
        return getLoadingStatsNative(nativePtr);
    }
    
    /**
     * Check if specific components are currently cached
     */
    public Map<IndexComponent, Boolean> getComponentCacheStatus() {
        return getComponentCacheStatusNative(nativePtr);
    }
    
    /**
     * Force eviction of specified components from cache
     */
    public void evictComponents(IndexComponent... components) {
        evictComponentsNative(nativePtr, components);
    }
    
    /**
     * Get list of files contained in this split
     */
    public List<String> listSplitFiles() {
        return listSplitFilesNative(nativePtr);
    }
    
    /**
     * Validate the integrity of the split file
     */
    public boolean validateSplit() {
        return validateSplitNative(nativePtr);
    }
    
    /**
     * Get split metadata including size, component information, etc.
     */
    public SplitMetadata getSplitMetadata() {
        return getSplitMetadataNative(nativePtr);
    }
    
    public static class SplitMetadata {
        private final String splitId;
        private final long totalSize;
        private final long hotCacheSize;
        private final int numComponents;
        private final Map<String, Long> componentSizes;
        
        public SplitMetadata(String splitId, long totalSize, long hotCacheSize, 
                           int numComponents, Map<String, Long> componentSizes) {
            this.splitId = splitId;
            this.totalSize = totalSize;
            this.hotCacheSize = hotCacheSize;
            this.numComponents = numComponents;
            this.componentSizes = new HashMap<>(componentSizes);
        }
        
        public String getSplitId() { return splitId; }
        public long getTotalSize() { return totalSize; }
        public long getHotCacheSize() { return hotCacheSize; }
        public int getNumComponents() { return numComponents; }
        public Map<String, Long> getComponentSizes() { return componentSizes; }
        public double getHotCacheRatio() { return (double) hotCacheSize / totalSize; }
    }
    
    @Override
    public void close() {
        if (nativePtr != 0) {
            closeNative(nativePtr);
            nativePtr = 0;
        }
    }
    
    // Native methods
    private static native long createNative(SplitSearchConfig config);
    private static native long getSchemaFromNative(long nativePtr);
    private static native SearchResult searchNative(long nativePtr, long queryPtr, int limit);
    private static native void preloadComponentsNative(long nativePtr, IndexComponent[] components);
    private static native CacheStats getCacheStatsNative(long nativePtr);
    private static native LoadingStats getLoadingStatsNative(long nativePtr);
    private static native Map<IndexComponent, Boolean> getComponentCacheStatusNative(long nativePtr);
    private static native void evictComponentsNative(long nativePtr, IndexComponent[] components);
    private static native List<String> listSplitFilesNative(long nativePtr);
    private static native boolean validateSplitNative(long nativePtr);
    private static native SplitMetadata getSplitMetadataNative(long nativePtr);
    private static native void closeNative(long nativePtr);
}