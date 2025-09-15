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
        System.out.println("🔍 Java SplitSearcher.getSchema called with nativePtr=" + nativePtr);
        long schemaPtr = getSchemaFromNative(nativePtr);
        System.out.println("🔍 Java getSchemaFromNative returned schemaPtr=" + schemaPtr);
        return new Schema(schemaPtr);
    }
    
    /**
     * Parse a query string using Quickwit's proven query parser.
     * This method leverages Quickwit's robust query parsing libraries for reliable
     * and efficient query parsing with the split's schema.
     * 
     * @param queryString The query string (e.g., "title:hello", "age:[1 TO 100]", "status:active AND priority:high")
     * @return A SplitQuery that can be used for efficient split searching
     */
    public SplitQuery parseQuery(String queryString) {
        Schema schema = getSchema();
        return SplitQuery.parseQuery(queryString, schema);
    }
    
    /**
     * Search the split using a SplitQuery with efficient QueryAst conversion.
     * This method converts the SplitQuery to Quickwit's QueryAst format and uses
     * Quickwit's proven search algorithms for optimal performance.
     * 
     * @param splitQuery The query to execute (use parseQuery() to create from string)
     * @param limit Maximum number of results to return
     * @return SearchResult containing matching documents and their scores
     */
    public SearchResult search(SplitQuery splitQuery, int limit) {
        if (nativePtr == 0) {
            throw new IllegalStateException("SplitSearcher has been closed or not properly initialized");
        }
        if (splitQuery == null) {
            throw new IllegalArgumentException("SplitQuery cannot be null");
        }
        
        try {
            // Pass SplitQuery object directly to native layer for efficient QueryAst conversion
            return searchWithSplitQuery(nativePtr, splitQuery, limit);
        } catch (Exception e) {
            throw new RuntimeException("Search failed: " + e.getMessage(), e);
        }
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
     * Retrieve multiple documents by their addresses in a single batch operation.
     * This is significantly more efficient than calling doc() multiple times,
     * especially for large numbers of documents.
     * 
     * @param docAddresses List of document addresses
     * @return List of documents in the same order as the input addresses
     */
    public List<Document> docBatch(List<DocAddress> docAddresses) {
        if (docAddresses == null || docAddresses.isEmpty()) {
            return new ArrayList<>();
        }
        
        // Convert to arrays for JNI transfer
        int[] segments = new int[docAddresses.size()];
        int[] docIds = new int[docAddresses.size()];
        
        for (int i = 0; i < docAddresses.size(); i++) {
            DocAddress addr = docAddresses.get(i);
            segments[i] = addr.getSegmentOrd();
            docIds[i] = addr.getDoc();
        }
        
        // Call native batch retrieval
        Document[] docs = docBatchNative(nativePtr, segments, docIds);
        return Arrays.asList(docs);
    }
    
    /**
     * Retrieve multiple documents efficiently using zero-copy semantics.
     * This method serializes all requested documents into a single ByteBuffer
     * for optimal performance when retrieving many documents.
     * 
     * The returned ByteBuffer uses the same binary protocol as batch document
     * indexing but in reverse - for efficient bulk document retrieval.
     * 
     * @param docAddresses List of document addresses to retrieve
     * @return ByteBuffer containing serialized documents, or null if addresses is empty
     * @throws RuntimeException if retrieval fails
     */
    public java.nio.ByteBuffer docsBulk(List<DocAddress> docAddresses) {
        if (docAddresses == null || docAddresses.isEmpty()) {
            return null;
        }
        
        // Convert to arrays for JNI transfer
        int[] segments = new int[docAddresses.size()];
        int[] docIds = new int[docAddresses.size()];
        
        for (int i = 0; i < docAddresses.size(); i++) {
            DocAddress addr = docAddresses.get(i);
            segments[i] = addr.getSegmentOrd();
            docIds[i] = addr.getDoc();
        }
        
        byte[] result = docsBulkNative(nativePtr, segments, docIds);
        return result != null ? java.nio.ByteBuffer.wrap(result) : null;
    }
    
    /**
     * Parse documents from a bulk retrieval ByteBuffer.
     * This method provides a convenient way to extract individual Document objects
     * from the zero-copy ByteBuffer returned by docsBulk().
     * 
     * @param buffer ByteBuffer from docsBulk() call
     * @return List of Document objects in the same order as the original request
     * @throws RuntimeException if parsing fails
     */
    public List<Document> parseBulkDocs(java.nio.ByteBuffer buffer) {
        if (buffer == null) {
            return new ArrayList<>();
        }
        
        return parseBulkDocsNative(buffer);
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
     * Warmup system: Proactively fetch query-relevant components before search execution.
     * This implements Quickwit's sophisticated warmup optimization that analyzes queries
     * and preloads only the data needed for optimal performance.
     * 
     * Benefits:
     * - Parallel async loading of query-relevant components
     * - Reduced search latency through proactive data fetching  
     * - Network request consolidation and caching optimization
     * - Memory-efficient loading of only required data
     * 
     * Usage:
     * <pre>{@code
     * Query query = Query.termQuery(schema, "title", "search term");
     * searcher.warmupQuery(query).thenCompose(v -> 
     *     searcher.searchAsync(query, 10)
     * ).thenAccept(results -> {
     *     // Search executes much faster due to pre-warmed data
     *     System.out.println("Results: " + results.getHits().size());
     * });
     * }</pre>
     * 
     * @param query The query to analyze and warm up components for
     * @return CompletableFuture that completes when warmup is finished
     * @throws RuntimeException if warmup fails
     */
    public CompletableFuture<Void> warmupQuery(Query query) {
        return CompletableFuture.runAsync(() -> {
            warmupQueryNative(nativePtr, query.getNativePtr());
        });
    }
    
    /**
     * Advanced warmup with explicit component control.
     * Allows fine-grained control over which components to warm up for a query.
     * 
     * @param query The query to warm up for
     * @param components Specific components to prioritize (null for auto-detection)
     * @param enableParallel Whether to use parallel loading (recommended: true)
     * @return CompletableFuture that completes when warmup is finished
     */
    public CompletableFuture<WarmupStats> warmupQueryAdvanced(Query query, IndexComponent[] components, boolean enableParallel) {
        return CompletableFuture.supplyAsync(() -> {
            return warmupQueryAdvancedNative(nativePtr, query.getNativePtr(), components, enableParallel);
        });
    }
    
    /**
     * Statistics about warmup performance and effectiveness
     */
    public static class WarmupStats {
        private final long totalBytesLoaded;
        private final long warmupTimeMs;
        private final int componentsLoaded;
        private final Map<IndexComponent, Long> componentSizes;
        private final boolean usedParallelLoading;
        
        public WarmupStats(long totalBytesLoaded, long warmupTimeMs, int componentsLoaded, 
                          Map<IndexComponent, Long> componentSizes, boolean usedParallelLoading) {
            this.totalBytesLoaded = totalBytesLoaded;
            this.warmupTimeMs = warmupTimeMs;
            this.componentsLoaded = componentsLoaded;
            this.componentSizes = new HashMap<>(componentSizes);
            this.usedParallelLoading = usedParallelLoading;
        }
        
        public long getTotalBytesLoaded() { return totalBytesLoaded; }
        public long getWarmupTimeMs() { return warmupTimeMs; }
        public int getComponentsLoaded() { return componentsLoaded; }
        public Map<IndexComponent, Long> getComponentSizes() { return componentSizes; }
        public boolean isUsedParallelLoading() { return usedParallelLoading; }
        public double getLoadingSpeedMBps() { 
            return warmupTimeMs > 0 ? (totalBytesLoaded / 1024.0 / 1024.0) / (warmupTimeMs / 1000.0) : 0.0;
        }
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

    /**
     * Tokenize a string using the proper tokenizer for the specified field.
     * This method uses the field's schema configuration to determine the correct
     * tokenizer and returns the list of tokens that would be generated for indexing.
     *
     * @param fieldName The name of the field (must exist in the schema)
     * @param text The text to tokenize
     * @return List of tokens generated by the field's tokenizer
     * @throws IllegalArgumentException if the field doesn't exist or text is null
     */
    public List<String> tokenize(String fieldName, String text) {
        if (nativePtr == 0) {
            throw new IllegalStateException("SplitSearcher has been closed or not properly initialized");
        }
        if (fieldName == null || fieldName.trim().isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        if (text == null) {
            throw new IllegalArgumentException("Text to tokenize cannot be null");
        }

        try {
            return tokenizeNative(nativePtr, fieldName, text);
        } catch (Exception e) {
            throw new RuntimeException("Tokenization failed for field '" + fieldName + "': " + e.getMessage(), e);
        }
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
    private static native SearchResult searchWithQueryAst(long nativePtr, String queryAstJson, int limit);
    private static native SearchResult searchWithSplitQuery(long nativePtr, SplitQuery splitQuery, int limit);
    private static native Document docNative(long nativePtr, int segment, int docId);
    private static native Document[] docBatchNative(long nativePtr, int[] segments, int[] docIds);
    private static native byte[] docsBulkNative(long nativePtr, int[] segments, int[] docIds);
    private static native List<Document> parseBulkDocsNative(java.nio.ByteBuffer buffer);
    private static native void preloadComponentsNative(long nativePtr, IndexComponent[] components);
    private static native void warmupQueryNative(long nativePtr, long queryPtr);
    private static native WarmupStats warmupQueryAdvancedNative(long nativePtr, long queryPtr, IndexComponent[] components, boolean enableParallel);
    private static native CacheStats getCacheStatsNative(long nativePtr);
    private static native LoadingStats getLoadingStatsNative(long nativePtr);
    private static native Map<IndexComponent, Boolean> getComponentCacheStatusNative(long nativePtr);
    private static native void evictComponentsNative(long nativePtr, IndexComponent[] components);
    private static native List<String> listSplitFilesNative(long nativePtr);
    private static native boolean validateSplitNative(long nativePtr);
    private static native SplitMetadata getSplitMetadataNative(long nativePtr);
    private static native List<String> tokenizeNative(long nativePtr, String fieldName, String text);
    private static native void closeNative(long nativePtr);
}