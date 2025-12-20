package io.indextables.tantivy4java.config;

/**
 * Global cache configuration for the Tantivy4Java library.
 * 
 * This class allows configuring the global caches that are shared across all
 * split searcher instances, following Quickwit's architecture.
 * 
 * The configuration must be set BEFORE creating any searchers or indexes.
 * Once set, the configuration cannot be changed.
 */
public class GlobalCacheConfig {
    
    // Default values matching Rust defaults
    private long fastFieldCacheMB = 1024; // 1GB
    private long splitFooterCacheMB = 500; // 500MB
    private long partialRequestCacheMB = 64; // 64MB
    private int maxConcurrentSplits = 100;
    private long aggregationMemoryMB = 500; // 500MB
    private int aggregationBucketLimit = 65000;
    private long warmupMemoryGB = 100; // 100GB
    
    // Split cache configuration (optional)
    private long splitCacheGB = 10; // 10GB default
    private int splitCacheMaxSplits = 10000;
    private String splitCachePath = null; // null means use temp directory

    // Searcher cache configuration
    private int searcherCacheSize = 50000; // 50,000 searcher entries default

    // Prescan parallelism configuration
    private int prescanParallelism = 16; // 16 concurrent prescan operations default
    
    private static boolean initialized = false;
    
    /**
     * Create a new global cache configuration with default values.
     */
    public GlobalCacheConfig() {
    }
    
    /**
     * Set the fast field cache capacity in MB.
     * Default: 1024 MB (1 GB)
     */
    public GlobalCacheConfig withFastFieldCacheMB(long mb) {
        this.fastFieldCacheMB = mb;
        return this;
    }
    
    /**
     * Set the split footer cache capacity in MB.
     * Default: 500 MB
     */
    public GlobalCacheConfig withSplitFooterCacheMB(long mb) {
        this.splitFooterCacheMB = mb;
        return this;
    }
    
    /**
     * Set the partial request cache capacity in MB.
     * Default: 64 MB
     */
    public GlobalCacheConfig withPartialRequestCacheMB(long mb) {
        this.partialRequestCacheMB = mb;
        return this;
    }
    
    /**
     * Set the maximum number of concurrent split searches.
     * Default: 100
     */
    public GlobalCacheConfig withMaxConcurrentSplits(int max) {
        this.maxConcurrentSplits = max;
        return this;
    }
    
    /**
     * Set the aggregation memory limit in MB.
     * Default: 500 MB
     */
    public GlobalCacheConfig withAggregationMemoryMB(long mb) {
        this.aggregationMemoryMB = mb;
        return this;
    }
    
    /**
     * Set the aggregation bucket limit.
     * Default: 65000
     */
    public GlobalCacheConfig withAggregationBucketLimit(int limit) {
        this.aggregationBucketLimit = limit;
        return this;
    }
    
    /**
     * Set the warmup memory budget in GB.
     * Default: 100 GB
     */
    public GlobalCacheConfig withWarmupMemoryGB(long gb) {
        this.warmupMemoryGB = gb;
        return this;
    }
    
    /**
     * Set the split cache size in GB.
     * Set to 0 to disable split cache.
     * Default: 10 GB
     */
    public GlobalCacheConfig withSplitCacheGB(long gb) {
        this.splitCacheGB = gb;
        return this;
    }
    
    /**
     * Set the maximum number of splits in the cache.
     * Default: 10000
     */
    public GlobalCacheConfig withSplitCacheMaxSplits(int max) {
        this.splitCacheMaxSplits = max;
        return this;
    }
    
    /**
     * Set the split cache root directory path.
     * If null, a temporary directory will be used.
     * Default: null (temp directory)
     */
    public GlobalCacheConfig withSplitCachePath(String path) {
        this.splitCachePath = path;
        return this;
    }

    /**
     * Set the searcher cache size (number of cached Tantivy Searcher instances).
     * This is an LRU cache that caches searchers by split URL.
     * Default: 50000 entries
     */
    public GlobalCacheConfig withSearcherCacheSize(int size) {
        this.searcherCacheSize = size;
        return this;
    }

    /**
     * Set the prescan parallelism (number of concurrent prescan operations).
     * This controls how many splits are checked in parallel during prescan.
     * Default: 16 concurrent operations
     */
    public GlobalCacheConfig withPrescanParallelism(int parallelism) {
        this.prescanParallelism = parallelism;
        return this;
    }

    /**
     * Initialize the global cache with this configuration.
     * This can only be called once. Subsequent calls will be ignored.
     * 
     * @return true if initialization succeeded, false if already initialized
     */
    public synchronized boolean initialize() {
        if (initialized) {
            return false;
        }
        
        boolean success = initializeGlobalCache(
            fastFieldCacheMB,
            splitFooterCacheMB,
            partialRequestCacheMB,
            maxConcurrentSplits,
            aggregationMemoryMB,
            aggregationBucketLimit,
            warmupMemoryGB,
            splitCacheGB,
            splitCacheMaxSplits,
            splitCachePath,
            searcherCacheSize,
            prescanParallelism
        );
        
        if (success) {
            initialized = true;
        }
        
        return success;
    }
    
    /**
     * Check if the global cache has been initialized.
     */
    public static boolean isInitialized() {
        return initialized;
    }
    
    // Native method
    private static native boolean initializeGlobalCache(
        long fastFieldCacheMB,
        long splitFooterCacheMB,
        long partialRequestCacheMB,
        int maxConcurrentSplits,
        long aggregationMemoryMB,
        int aggregationBucketLimit,
        long warmupMemoryGB,
        long splitCacheGB,
        int splitCacheMaxSplits,
        String splitCachePath,
        int searcherCacheSize,
        int prescanParallelism
    );
    
    static {
        System.loadLibrary("tantivy4java");
    }
}