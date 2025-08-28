package com.tantivy4java;

import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * SplitSearcher provides efficient search capabilities over Quickwit split files
 * with hot cache optimization and incremental loading of index components.
 * 
 * <h3>Shared Cache Architecture</h3>
 * SplitSearcher instances MUST be created through {@link SplitCacheManager#createSplitSearcher(String)}
 * to ensure proper shared cache management. All SplitSearcher instances created through the same
 * SplitCacheManager share global caches for:
 * <ul>
 *   <li>LeafSearchCache - Query result caching per split_id + query</li>
 *   <li>ByteRangeCache - Storage byte range caching per file_path + range</li>
 *   <li>ComponentCache - Index component caching (fast fields, postings, etc.)</li>
 * </ul>
 * 
 * <h3>Proper Usage Pattern</h3>
 * <pre>{@code
 * // Create shared cache manager (reuse across application)
 * SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("main-cache")
 *     .withMaxCacheSize(200_000_000); // 200MB shared across all splits
 * SplitCacheManager cacheManager = SplitCacheManager.getInstance(config);
 * 
 * // Create searchers that share the cache
 * SplitSearcher searcher1 = cacheManager.createSplitSearcher("s3://bucket/split1.split");
 * SplitSearcher searcher2 = cacheManager.createSplitSearcher("s3://bucket/split2.split");
 * }</pre>
 */
public class SplitSearcher implements AutoCloseable {
    
    static {
        Tantivy.initialize();
    }

    private long nativePtr;
    private final String splitPath;
    // Configuration now comes from SplitCacheManager
    private final SplitCacheManager cacheManager;
    
    
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
    
    /**
     * Constructor for SplitSearcher (now always uses shared cache)
     */
    SplitSearcher(String splitPath, SplitCacheManager cacheManager, Map<String, Object> splitConfig) {
        this.splitPath = splitPath;
        this.cacheManager = cacheManager;
        this.nativePtr = createNativeWithSharedCache(splitPath, cacheManager.getNativePtr(), splitConfig);
    }
    
    /**
     * @deprecated REMOVED - Use SplitCacheManager.createSplitSearcher() instead.
     * 
     * Proper usage pattern:
     * <pre>{@code
     * SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("my-cache")
     *     .withMaxCacheSize(200_000_000);
     * SplitCacheManager cacheManager = SplitCacheManager.getInstance(config);
     * SplitSearcher searcher = cacheManager.createSplitSearcher(splitPath);
     * }</pre>
     * 
     * This ensures proper shared cache management across all splits.
     */
    @Deprecated
    private static SplitSearcher create(String splitPath) {
        throw new UnsupportedOperationException(
            "SplitSearcher.create() is deprecated. Use SplitCacheManager.createSplitSearcher() instead. " +
            "Create a SplitCacheManager with proper configuration and use it to create searchers."
        );
    }
    
    /**
     * Get the schema for this split
     */
    public Schema getSchema() {
        return new Schema(getSchemaFromNative(nativePtr));
    }
    
    /**
     * Parse a query string using this split's schema and tokenization settings.
     * This method provides the same query parsing functionality as Index.parseQuery()
     * but uses the schema from the split file.
     * 
     * @param queryString The query string to parse (e.g., "title:python AND content:machine")
     * @return Parsed Query object ready for search operations
     * @throws RuntimeException If the query string is malformed or contains invalid field references
     * 
     * @see Index#parseQuery(String)
     */
    public Query parseQuery(String queryString) {
        long queryPtr = parseQueryNative(nativePtr, queryString);
        return new Query(queryPtr);
    }
    
    /**
     * Search the split with hot cache optimization
     */
    public SearchResult search(Query query, int limit) {
        return searchNative(nativePtr, query.getNativePtr(), limit);
    }
    
    /**
     * Retrieve a document by its address from the split
     * @param docAddress The document address obtained from search results
     * @return The document with all indexed fields
     */
    public Document doc(DocAddress docAddress) {
        return docNative(nativePtr, docAddress.getSegmentOrd(), docAddress.getDoc());
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
        
        // Always notify cache manager (all instances now use shared cache)
        cacheManager.removeSplitSearcher(splitPath);
    }
    
    // Native methods
    private static native long createNativeWithSharedCache(String splitPath, long cacheManagerPtr, Map<String, Object> splitConfig);
    private static native long getSchemaFromNative(long nativePtr);
    private static native long parseQueryNative(long nativePtr, String queryString);
    private static native SearchResult searchNative(long nativePtr, long queryPtr, int limit);
    private static native Document docNative(long nativePtr, int segment, int docId);
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